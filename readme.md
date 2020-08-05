## Google Cloud Playground

Quickly test Google Cloud services

### Prerequisites
Java Dev Env
- JDK8+
- Maven

You may also need to set the `JAVA_HOME` & `M2_HOME` in the `run.sh` script.

### Google Cloud java client
[Github](https://github.com/googleapis/google-cloud-java/tree/master/google-cloud-clients)

### [config file](https://github.com/bindiego/gcpplayground/blob/gcp/conf/config.properties)

This is the most important part of this project, most components are configured here.

You can specify which service account to use by simply set the path of the json file to load.

Once it has been configured. Run it :)

```
./run.sh
```

### Use http proxy for GCP java client libs

```
http.proxy = on
http.proxy.host = localhost
http.proxy.port = 7443
```

set it [here](https://github.com/cloudymoma/gcpplayground/blob/gcp/conf/config.properties#L7-L9)

### BigQuery

refer to [this Gist](https://gist.github.com/bindiego/17898d41e98fae201ce2c1d1da3ba9fc) for setting up the testing table.

[sample code](https://github.com/cloudymoma/gcpplayground/tree/gcp/src/main/java/org/bindiego/google/bq)

### Translate API

[sample code](https://github.com/cloudymoma/gcpplayground/tree/gcp/src/main/java/org/bindiego/google/translate)

### Pubsub

This project will create the Topic and the Subscription for you, you can simply turn this off by set

```
google.pubsub.skip.init = true
```

in the [config file](https://github.com/bindiego/gcpplayground/blob/gcp/conf/config.properties). If you don't do it while the topic and the subscription is there, it will show you a clear runtime exeception and keep running. 

You could also use the pubsub component as data generator for testing a [**streaming system** on Google Cloud](https://github.com/cloudymoma/raycom)

[sample code](https://github.com/cloudymoma/gcpplayground/tree/gcp/src/main/java/org/bindiego/google/pubsub)

### Pubsublite

[sample code](https://github.com/cloudymoma/gcpplayground/tree/gcp/src/main/java/org/bindiego/google/pubsub/lite). Similar to Pubsub with certain limits

## Cloud Storage, aka GCS

[sample code](https://github.com/cloudymoma/gcpplayground/tree/gcp/src/main/java/org/bindiego/google/gcs)
