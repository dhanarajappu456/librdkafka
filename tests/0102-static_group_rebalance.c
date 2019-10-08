/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2012-2019, Magnus Edenhill
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "test.h"


/**
 * @name KafkaConsumer static membership tests
 *
 * Runs two consumers subscribing to a topic simulating various
 * rebalance scenarios.
 */

#define _CONSUMER_CNT 2

typedef struct _consumer_s {
        rd_kafka_t *rk;
        test_msgver_t *mv;
        int assignment_cnt;
        int rebalance_cnt;
        int max_rebalance_cnt;
} _consumer_t;

/**
 * @brief performs work on each consumer in the group
 */
void do_each(_consumer_t *c, void (*fn)(_consumer_t *)) {
        int i;
        for (i = 0; i < _CONSUMER_CNT; i++, c++)
                fn(c);
}

/**
 * @brief Serves consumer queue until next revoke
 */
void static_member_wait_revoke(_consumer_t *c) {
        int last = c->rebalance_cnt;
        TEST_SAY("%s awaiting revoke \n", rd_kafka_name(c->rk));

         c->max_rebalance_cnt++;
         while(c->rebalance_cnt <= last)
                test_consumer_poll_once(c->rk, c->mv, 1000);
}

static void rebalance_cb (rd_kafka_t *rk,
                          rd_kafka_resp_err_t err,
                          rd_kafka_topic_partition_list_t *parts,
                          void *opaque) {
        _consumer_t *c = opaque;

        switch (err)
        {
        case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                c->assignment_cnt = parts->cnt;
                rd_kafka_assign(rk, parts);
                break;

        case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                TEST_SAY("%s partitions revoked\n", rd_kafka_name(c->rk));

                TEST_ASSERT(++c->rebalance_cnt == c->max_rebalance_cnt,
                    "%s rebalanced %d times, max was %d",
                    rd_kafka_name(rk),
                    c->rebalance_cnt, c->max_rebalance_cnt);

                rd_kafka_assign(rk, NULL);
                break;

        default:
                TEST_FAIL("rebalance failed: %s", rd_kafka_err2str(err));
                break;
        }

}

/**
 * @brief Terminating consumer should revoke partitions without prompting
 *  a group-wide rebalance distrubing other members
*/
void static_member_destroy (_consumer_t *c) {
        TEST_SAY("Closing consumer %s\n", rd_kafka_name(c->rk));

        c->max_rebalance_cnt++;
        test_consumer_close(c->rk);
        rd_kafka_destroy(c->rk);
}

/**
 * @brief Restarts a consumer group member
 */
void static_member_restart (_consumer_t *c) {
        rd_kafka_topic_partition_list_t *partitions;
        rd_kafka_conf_t *conf;

        conf = rd_kafka_conf_dup(rd_kafka_conf(c->rk));
        rd_kafka_subscription(c->rk, &partitions);

        static_member_destroy(c);

        c->rk = test_create_handle(RD_KAFKA_CONSUMER, conf);
        rd_kafka_poll_set_consumer(c->rk);
        rd_kafka_subscribe(c->rk, partitions);

        test_consumer_wait_assignment(c->rk, c->mv);
}

int main_0102_static_group_rebalance (int argc, char **argv) {
        rd_kafka_conf_t *conf;
        test_msgver_t mv;

        _consumer_t c[_CONSUMER_CNT] = RD_ZERO_INIT;
        const int msgcnt = 100;
        uint64_t testid  = test_id_generate();
        const char *topic = test_mk_topic_name(
                                               "0102_static_group_rebalance",
                                               1);
        char *topics = rd_strdup(tsprintf("^%s.*", topic));

        test_msgver_init(&mv, testid);
        c[0].mv = &mv;
        c[1].mv = &mv;

        test_create_topic(NULL, topic, 3, 1);
        test_produce_msgs_easy(topic, testid, RD_KAFKA_PARTITION_UA, msgcnt);

        test_conf_init(&conf, NULL, 60);
        test_conf_set(conf, "session.timeout.ms", "5000");
        test_conf_set(conf, "max.poll.interval.ms", "10001");
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "topic.metadata.refresh.interval.ms", "500");
        test_conf_set(conf, "enable.partition.eof", "true");
        test_conf_set(conf, "isolation.level", "read_uncommitted");

        rd_kafka_conf_set_opaque(conf, &c[0]);
        test_conf_set(conf, "group.instance.id", "consumer1");
        c[0].rk = test_create_consumer(topic, rebalance_cb,
                                       rd_kafka_conf_dup(conf), NULL);

        rd_kafka_conf_set_opaque(conf, &c[1]);
        test_conf_set(conf, "group.instance.id", "consumer2");
        c[1].rk = test_create_consumer(topic, rebalance_cb,
                                       rd_kafka_conf_dup(conf), NULL);

        test_consumer_subscribe(c[0].rk, topics);
        test_consumer_subscribe(c[1].rk, topics);

        test_consumer_wait_assignment(c[0].rk, &mv);
        test_consumer_wait_assignment(c[1].rk, &mv);

        /*
         * Consume all the messages so we can watch for duplicates
         * after rejoin/rebalance operations.
         */
        test_consumer_poll("serve.queue", 
                           c[0].rk, testid, c[0].assignment_cnt, 0, -1, &mv);
        test_consumer_poll("serve.queue", 
                           c[1].rk, testid, c[1].assignment_cnt, 0, -1, &mv);

        test_msgver_verify("first.consume", &mv, TEST_MSGVER_ALL, 0, msgcnt);

        /* Leave and rejoin group without rebalancing group */
        TEST_SAY("Bouncing %s\n", rd_kafka_name(c[1].rk));
        static_member_restart(&c[1]);

        /* Poll c[0] to ensure revoke cb was not queued */
        test_consumer_poll_no_msgs("static.restart", c[0].rk, testid, 1000);

        /* Subscription expansions should rebalance the group */
        TEST_SAY("Expanding subscription with new topic\n");
        test_create_topic(c->rk, tsprintf("%snew", topic), 1, 1);

        do_each(c, &static_member_wait_revoke);
        test_consumer_wait_assignment(c[0].rk, &mv);
        test_consumer_wait_assignment(c[1].rk, &mv);

        /* Unsubscribe should rebalance the group */
        TEST_SAY("Testing consumer Unsubscribe\n");
        rd_kafka_unsubscribe(c[0].rk);
        do_each(c, static_member_wait_revoke);
        test_consumer_subscribe(c[0].rk, topics);

        /* 
         * Static members continue to enforce `max.poll.interval.ms`. 
         * These members remain in the member list however so we  must 
         * interleave calls to poll while awaiting our assignment to avoid
         * unexpected rebalances being triggered.
         */
        while(test_consumer_wait_timed_assignment(c[0].rk, &mv, 1000)) {
                test_consumer_poll_no_msgs("test", c[1].rk, testid, 1000);
        }
        test_consumer_wait_assignment(c[1].rk, &mv);

        TEST_SAY("c[0] coercing c[1] max poll violation");
        c[0].max_rebalance_cnt++;
        test_consumer_poll_no_msgs("wait.max.poll", c[0].rk, testid, 15000);
        c[1].max_rebalance_cnt++;
        test_consumer_poll_expect_err(c[1].rk, testid, 1000, 
                                      RD_KAFKA_RESP_ERR__MAX_POLL_EXCEEDED);

        /* c[1] rejoin group  */
        test_consumer_poll_no_msgs("wait.max.poll", c[1].rk, testid, 500);
        /* Wait until session.timeout.ms is exceeded to force a rebalance. */
        TEST_SAY("Closing c[1], waiting for static instance to be evicted.\n");
        static_member_destroy(&c[1]);
        static_member_wait_revoke(&c[0]);

        TEST_SAY("Verifying message consumption and closing consumer\n");
        TEST_ASSERT(mv.msgcnt == msgcnt, "Only %d of %d messages were consumed\n", mv.msgcnt, msgcnt);
        static_member_destroy(&c[0]);

        return 0;
}
