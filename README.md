# scheduledmessage

This is an experiment.

It's basically something like "at" for kafka.  It receives messages on a scheduler 
specific topic and will send the payload to another topic when the supplied date has passed.  

It's built using quartz, so it can be clustered via the database if desired.