package mflix.api.daos;

import com.mongodb.MongoClientSettings;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import mflix.api.models.Session;
import mflix.api.models.User;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@Configuration
public class UserDao extends AbstractMFlixDao {

    private final MongoCollection<User> usersCollection;
    private final MongoCollection<Session> sessionsCollection;

    private final Logger log;

    @Autowired
    public UserDao(
            MongoClient mongoClient, @Value("${spring.mongodb.database}") String databaseName) {
        super(mongoClient, databaseName);
        CodecRegistry pojoCodecRegistry =
                fromRegistries(
                        MongoClientSettings.getDefaultCodecRegistry(),
                        fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        usersCollection = db.getCollection("users", User.class).withCodecRegistry(pojoCodecRegistry);
        sessionsCollection = db.getCollection("sessions", Session.class).withCodecRegistry(pojoCodecRegistry);
        log = LoggerFactory.getLogger(this.getClass());
    }

    /**
     * Inserts the `user` object in the `users` collection.
     *
     * @param user - User object to be added
     * @return True if successful, throw IncorrectDaoOperation otherwise
     */
    public boolean addUser(User user) {
        Bson filter = eq("email", user.getEmail());
        FindIterable<User> users = usersCollection.find(filter);
        if (users.first() != null) {
            log.info("User with email {} already exists", user.getEmail());
            throw new IncorrectDaoOperation("User is already created");
        }
        usersCollection.withWriteConcern(WriteConcern.MAJORITY).insertOne(user);
        return true;
    }

    /**
     * Creates session using userId and jwt token.
     *
     * @param userId - user string identifier
     * @param jwt    - jwt string token
     * @return true if successful
     */
    public boolean createUserSession(String userId, String jwt) {
        Document document = new Document();
        document.put("user_id", userId);
        document.put("jwt", jwt);
        if (sessionsCollection.find(document).first() != null) {
            log.info("Session with jwt {} already exists", jwt);
            return false;
        }
        Session session = new Session();
        session.setUserId(userId);
        session.setJwt(jwt);
        sessionsCollection.replaceOne(eq("user_id", userId), session);
        return true;
    }

    /**
     * Returns the User object matching the an email string value.
     *
     * @param email - email string to be matched.
     * @return User object or null.
     */
    public User getUser(String email) {
        return usersCollection.find(eq("email", email)).first();
    }

    /**
     * Given the userId, returns a Session object.
     *
     * @param userId - user string identifier.
     * @return Session object or null.
     */
    public Session getUserSession(String userId) {
        return sessionsCollection.find(eq("user_id", userId)).first();
    }

    public boolean deleteUserSessions(String userId) {
        return sessionsCollection.deleteMany(eq("user_id", userId)).wasAcknowledged();
    }

    /**
     * Removes the user document that match the provided email.
     *
     * @param email - of the user to be deleted.
     * @return true if user successfully removed
     */
    public boolean deleteUser(String email) {
        this.deleteUserSessions(email);
        return usersCollection.deleteOne(eq("email", email)).wasAcknowledged();
    }

    /**
     * Updates the preferences of an user identified by `email` parameter.
     *
     * @param email           - user to be updated email
     * @param userPreferences - set of preferences that should be stored and replace the existing
     *                        ones. Cannot be set to null value
     * @return User object that just been updated.
     */
    public boolean updateUserPreferences(String email, Map<String, ?> userPreferences) {
        Bson filter = eq("email", email);
        User user = usersCollection.find(filter).first();
        if (user == null) {
            log.warn("No user with email {}", email);
            return false;
        }
        if (userPreferences == null) {
            throw new IncorrectDaoOperation("User preferences cannot be NULL");
        }
        usersCollection.updateOne(filter, set("preferences", userPreferences));
        return true;
    }
}
