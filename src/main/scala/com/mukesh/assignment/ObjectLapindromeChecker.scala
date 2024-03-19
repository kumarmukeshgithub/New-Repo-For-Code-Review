package com.mukesh.assignment
import scala.io.StdIn.readLine

object ObjectLapindromeChecker {
  def main(args:Array[String]): Unit = {
    //Taking input from user at run time.
    println("Please enter a string:")
    val userInput = readLine().toLowerCase().replaceAll("[^a-z]", "")

    // Remove the middle character if the length is odd
    val filteredInput = if (userInput.length % 2 != 0) userInput.patch(userInput.length / 2, "", 1) else userInput

    // Split the string into two equal halves
    val (firstHalf, secondHalf) =filteredInput.splitAt(filteredInput.length / 2)

    // Reverse the second half of the string
    val reversedRightHalf = secondHalf.reverse

    // Printing the result as per the requirment
    if (firstHalf == reversedRightHalf) {
      println("Yes, the string is a lapindrome")
      println(s"The length of the lapindrome: ${userInput.length}")
      println(s"The middle character of the string: ${userInput.charAt(userInput.length / 2)}")

      println(s"The left half of the lapindrome: $firstHalf")
      println(s"The right half of the lapindrome: $secondHalf")
      println(s"The reversed right half of the lapindrome: $reversedRightHalf")
    } else {
      println("No, the string is not a lapindrome")
    }
  }

}
