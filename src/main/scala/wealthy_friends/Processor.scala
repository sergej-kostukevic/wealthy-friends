package wealthy_friends

import org.apache.spark.sql.{Dataset, SparkSession}
import wealthy_friends.model.{User, UserFriendsMoney, UserSingleFriend}

class Processor(spark: SparkSession, inputDir: String) {
  import spark.implicits._

  /* Transforming raw text data into strong-type structure */
  val rawData: Dataset[String] = spark.read.textFile(inputDir)
  val rawSeparatedData: Dataset[List[String]] = rawData.map(_.split(',').toList)
  val preparedData: Dataset[User] = rawSeparatedData
    .filter(values => values.size > 2 && values(1).toIntOption.isDefined) // we are not interested in users without friends && checking if the money value is integer
    .map {
      case name :: moneyStr :: friends => User(name, moneyStr.toInt, friends)
      case _ => User("", 0, List.empty) // this case should never happen as it filtered above
    }

  // Making a structure flatter - a user with a list of friends became to a list of user with only one friend
  val singleFriendData: Dataset[UserSingleFriend] = preparedData.flatMap(
    user => user.friends.map(friend => UserSingleFriend(user.name, user.money, friend))
  ).persist() // Persisted just if you want to try both methods findUser_ScalaWay() and findUser_SqlWay() at once

  def findUser_ScalaWay(): String = {
    // The main goal is to pair a user with friends including their money
    val pairedPeople: Dataset[(UserSingleFriend, UserSingleFriend)] = singleFriendData.as("user").joinWith(singleFriendData.as("friend"), $"user.friend" === $"friend.name")

    // Joining tables with a many-to-many relation brings too many pairs
    // taking only user name and friend's money combined with distinct leaves significant and only significant information
    val userWithFriendsMoney: Dataset[UserFriendsMoney] = pairedPeople.map(userAndFriend => UserFriendsMoney(userAndFriend._1.name, userAndFriend._2.money)).distinct()

    // Finally, aggregation of friends' money
    val userWithAllFriendsMoney: Dataset[UserFriendsMoney] = userWithFriendsMoney.groupByKey(_.name).mapGroups((k, v) => UserFriendsMoney(k, v.foldLeft(0L)((acc, u) => acc + u.friendsMoney)))

    // Taking the highest amount and returning a user name
    userWithAllFriendsMoney.sort($"friendsMoney".desc).head().name
  }

  def findUser_SqlWay(): String = {

    singleFriendData.createOrReplaceTempView("singleFriendData")

    val query: String = """
SELECT userWithFriendsMoney.name AS name, SUM(userWithFriendsMoney.money) AS friendsMoney
FROM (
  SELECT DISTINCT user.name AS name, friend.money AS money
  FROM
    singleFriendData AS user
  JOIN
    singleFriendData AS friend
  ON
    user.friend = friend.name
) userWithFriendsMoney
GROUP BY userWithFriendsMoney.name
ORDER BY friendsMoney DESC
LIMIT 1
"""

    spark.sql(query).as[UserFriendsMoney].head().name

  }
}

object Processor {
  def apply(spark: SparkSession, inputDir: String): Processor = new Processor(spark, inputDir)
}
