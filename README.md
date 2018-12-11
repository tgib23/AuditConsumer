# AuditConsumer

Sample program to consume audit stream and store them directly to MapR-DB JSON.


# Manual Execution

```bash
$ javac -cp .:`mapr classpath` AuditConsumer.java -Xlint:deprecation
$ java -cp .:`mapr classpath` AuditConsumer -cluster <cluster name> -output_db_path <output db path> -debug <0/1>
```

# Daemonize Consumer

```bash
$ javac -cp .:`mapr classpath` AuditConsumer.java -Xlint:deprecation
$ sudo mkdir /opt/mapr/audit_consumer
$ sudo cp AuditConsumer* consumer.props /opt/mapr/audit_consumer/
```

Then, edit AuditConsumer.service to specify the correct classpath, cluster name and output db path to 'ExecStart'

```
$ sudo cp AuditConsumer.service /etc/systemd/system/
$ sudo systemctl enable AuditConsumer
$ sudo systemctl start AuditConsumer
```

This will store audit consumed logs into specified db path.
