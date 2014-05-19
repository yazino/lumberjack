Lumberjack
==========

A very quick & dirty [http://logio.org](Log.io) harvester for [Logback](http://logback.qos.ch).

Use at your own risk :-)

It's pretty much a rip of Logback's SocketAppender, with the difference that we send Log.io style styles rather than serialised Java objects.

You'll probably want to use a [branch of Log.io](https://github.com/jshiell/Log.io) that applies white-space preservation to the message blocks.

Just slap an appender into your Logback XML:

```
<appender name="LOGIO" class="com.yazino.lumberjack.LogIoAppender">
    <RemoteHost>aHost</RemoteHost>
    <StreamName>aName</StreamName>

    <encoder>
        <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger - %msg%n</pattern>
    </encoder>
</appender>
```

To build, try:

```
gradle build
```

To push to the local Maven repo:

```
gradle publishToMavenLocal
```
