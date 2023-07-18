package org.softwareforce.iotvm.eventengine.kafka;

import com.jasongoodwin.monads.Try;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service for managing Kafka.
 *
 * @author Dimitris Gkoulis
 */
public class KafkaAdminService {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAdminService.class);

  private final Admin kafkaAdmin;

  /* ------------ Constructors ------------ */

  public KafkaAdminService(Admin kafkaAdmin) {
    this.kafkaAdmin = kafkaAdmin;
  }

  /* ------------ Utilities ------------ */

  public List<NewTopic> convertTopicNamesToNewTopics(
      List<String> topicNameList, final int defaultPartitions, final int defaultReplicationFactor) {
    return topicNameList.stream()
        .map(
            (topicName) ->
                new NewTopic(topicName, defaultPartitions, (short) defaultReplicationFactor))
        .toList();
  }

  /* ------------ Logic ------------ */

  public void createTopics(List<NewTopic> newTopicList) {
    LOGGER.info(
        "Request to create topics : {}",
        newTopicList.stream().map(NewTopic::name).collect(Collectors.joining(", ")));

    try {
      final CreateTopicsResult createTopicsResult = this.kafkaAdmin.createTopics(newTopicList);

      createTopicsResult
          .values()
          .forEach(
              (topicName, future) -> {
                future.whenComplete(
                    (aVoid, maybeError) ->
                        Optional.ofNullable(maybeError)
                            .map(Try::<Void>failure)
                            .orElse(Try.successful(null))
                            .onFailure(
                                throwable -> {
                                  if (throwable
                                      instanceof CompletionException completionException) {
                                    if (completionException.getCause() != null) {
                                      if (completionException.getCause()
                                          instanceof TopicExistsException) {
                                        LOGGER.warn(
                                            "Topic creation didn't complete: {} already exists!",
                                            topicName);
                                      }
                                    }
                                  } else if (throwable instanceof TopicExistsException) {
                                    LOGGER.warn(
                                        "Topic creation didn't complete: {} already exists!",
                                        topicName);
                                  } else {
                                    LOGGER.error("Topic creation didn't complete:", throwable);
                                  }
                                })
                            .onSuccess(
                                (anOtherVoid) ->
                                    LOGGER.info(
                                        String.format(
                                            "Topic %s, has been successfully created!",
                                            topicName))));
              });

      createTopicsResult.all().get();
    } catch (InterruptedException | ExecutionException e) {
      if (!(e.getCause() instanceof TopicExistsException)) {
        e.printStackTrace();
      }
    }
  }
}
