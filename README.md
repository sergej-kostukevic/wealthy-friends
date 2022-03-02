# The "Wealthy Friends" challenge

## Challenge description
Please complete the following task. You can use any language you prefer but using Java/Scala is preferable, and the usage of some big data technology (Hadoop/Spark) is more than welcome. A compilable/runnable code is required, runnable code with tests that we can quickly run to verify your submission is always preferred.

Please publish to Github or dropbox/google drive and e-mail us the link.


Input: one or more text files with lines with the following structure:

< name >, < money >, < friend1 >, < friend2 >, ... , < friendk >

< name > is unique across the whole dataset - imagine e.g. usernames.  
< money > is a non-negative integer - how much money does the person have. 
< friend1 > ... < friendk > is a set of friends that the given user has. Each user can have a different number of friends. If user A has a friend B, it does not imply that B has a friend A.

Output: username of the user with most wealthy friends. It means the user where the sum of money of his friends is the biggest among all the users. The money of the user himself is not counted, only of his friends.

The size of the input is so big that the code must run on a cluster of machines. Imagine the size of the Facebook user database.

Example:

Input:
```csv

david,10,petr,josef,andrea
andrea,50,josef,martin
petr,20,david,josef
josef,5,andrea,petr,david
martin,100,josef,andrea,david
```

Output:
```
andrea
```

## How to run
```shell
sbt
run --input-dir /absolute/path/to/the/input/directory/
```

## How it works
The [Processor](src/main/scala/wealthy_friends/Processor.scala) class provides two similar methods to find a solution
- __Processor.findUser_ScalaWay__ uses strongly typed Dataset API
- __Processor.findUser_SqlWay__ uses Spark SQL

See comments in the code for a more detailed description.

## Input data restrictions
Input data should not contain spaces of another noise before/after values, only names and commas are expected.

