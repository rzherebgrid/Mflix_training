package mflix.api.daos;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoWriteException;
import com.mongodb.ReadConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import mflix.api.models.Comment;
import mflix.api.models.Critic;
import mflix.api.models.User;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.text.MessageFormat;
import java.util.*;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;
import static org.springframework.util.StringUtils.isEmpty;

@Component
public class CommentDao extends AbstractMFlixDao {

    public static String COMMENT_COLLECTION = "comments";
    public static String USER_COLLECTION = "users";

    private final Logger log;
    private MongoCollection<Comment> commentCollection;
    private MongoCollection<User> userCollection;
    private CodecRegistry pojoCodecRegistry;

    @Autowired
    public CommentDao(
            MongoClient mongoClient, @Value("${spring.mongodb.database}") String databaseName) {
        super(mongoClient, databaseName);
        log = LoggerFactory.getLogger(this.getClass());
        this.db = this.mongoClient.getDatabase(MFLIX_DATABASE);
        this.pojoCodecRegistry =
                fromRegistries(
                        MongoClientSettings.getDefaultCodecRegistry(),
                        fromProviders(PojoCodecProvider.builder().automatic(true).build()));
        this.commentCollection =
                db.getCollection(COMMENT_COLLECTION, Comment.class).withCodecRegistry(pojoCodecRegistry);
        this.userCollection =
                db.getCollection(USER_COLLECTION, User.class).withCodecRegistry(pojoCodecRegistry);
    }

    /**
     * Returns a Comment object that matches the provided id string.
     *
     * @param id - comment identifier
     * @return Comment object corresponding to the identifier value
     */
    public Comment getComment(String id) {
        return commentCollection.find(new Document("_id", new ObjectId(id))).first();
    }

    /**
     * Adds a new Comment to the collection. The equivalent instruction in the mongo shell would be:
     *
     * <p>db.comments.insertOne({comment})
     *
     * <p>
     *
     * @param comment - Comment object.
     * @throw IncorrectDaoOperation if the insert fails, otherwise
     * returns the resulting Comment object.
     */
    public Comment addComment(Comment comment) {
        if (isEmpty(comment.getId())) {
            throw new IncorrectDaoOperation("Comment objects need to have an id field set.");
        }
        commentCollection.insertOne(comment);
        return comment;
    }

    /**
     * Updates the comment text matching commentId and user email. This method would be equivalent to
     * running the following mongo shell command:
     *
     * <p>db.comments.update({_id: commentId}, {$set: { "text": text, date: ISODate() }})
     *
     * <p>
     *
     * @param commentId - comment id string value.
     * @param text      - comment text to be updated.
     * @param email     - user email.
     * @return true if successfully updates the comment text.
     */
    public boolean updateComment(String commentId, String text, String email) {

        Bson filter = Filters.and(
                Filters.eq("email", email),
                Filters.eq("_id", new ObjectId(commentId)));
        Bson update = Updates.combine(Updates.set("text", text),
                Updates.set("date", new Date())) ;
        UpdateResult res = commentCollection.updateOne(filter, update);

        if(res.getMatchedCount() > 0){

            if (res.getModifiedCount() != 1){
                log.warn("Comment `{}` text was not updated. Is it the same text?", commentId);
            }

            return true;
        }
        log.error("Could not update comment `{}`. Make sure the comment is owned by `{}`",
                commentId, email);
        return false;
    }

    /**
     * Deletes comment that matches user email and commentId.
     *
     * @param commentId - commentId string value.
     * @param email     - user email value.
     * @return true if successful deletes the comment.
     */
    public boolean deleteComment(String commentId, String email) {
        if (StringUtils.isEmpty(commentId)) {
            throw new IllegalArgumentException();
        }
        Bson filter = Filters.and(Filters.eq("_id", new ObjectId(commentId)), Filters.eq("email", email));
        Comment commentDeleted = commentCollection.findOneAndDelete(filter);
        return commentDeleted != null;
    }

    /**
     * Ticket: User Report - produce a list of users that comment the most in the website. Query the
     * `comments` collection and group the users by number of comments. The list is limited to up most
     * 20 commenter.
     *
     * @return List {@link Critic} objects.
     */
    public List<Critic> mostActiveCommenters() {
        List<Critic> mostActive = new ArrayList<>();

        List<Bson> pipeline =  Arrays.asList(new Document("$lookup",
                        new Document("from", "comments")
                                .append("let",
                                        new Document("userName", "$name"))
                                .append("pipeline", Collections.singletonList(new Document("$match",
                                        new Document("$expr",
                                                new Document("$eq", Arrays.asList("$name", "$$userName"))))))
                                .append("as", "comments")),
                new Document("$addFields",
                        new Document("count",
                                new Document("$size", "$comments"))),
                new Document("$project",
                        new Document("count", 1L).append("_id", -1L).append("email", 1L)),
                new Document("$sort",
                        new Document("count", -1L)),
                new Document("$limit", 20L));
        userCollection.withReadConcern(ReadConcern.MAJORITY)
                 .aggregate(pipeline, Critic.class).spliterator().forEachRemaining(mostActive::add);
        return mostActive;
    }
}
