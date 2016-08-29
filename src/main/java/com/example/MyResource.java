package com.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import redis.clients.jedis.Jedis;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Root resource (exposed at "myresource" path)
 */
@Path("myresource")
public class MyResource {

    /**
     * Method handling HTTP GET requests. The returned object will be sent
     * to the client as "text/plain" media type.
     *
     * @return String that will be returned as a text/plain response.
     */

    @Inject
    KafkaProducer producer;

    @Inject
    Jedis jedis;

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public Response scheduleAnalysis(@QueryParam("url") String url) {
        producer.send(new ProducerRecord<String, String>("url_links", url));
        return Response.accepted().build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getWords() {
        // this will fail for large datasets
        Set<String> ignorekeys = jedis.keys("cluster*");
        Set<String> keys = jedis.keys("*");
        Map<String, String> db = new HashMap<>();
        for (String key : keys) {
            if (ignorekeys.contains(key))
                continue;
            String val = jedis.get(key);
            db.put(key, val);
        }
        return Response.ok().entity(db).build();
    }
}
