# spark-drools-playground

Just an attempt to mentor learners of Spark with Jboss Drools(https://www.drools.org/). 

What is Drools?
A: Drools is a Business Rules Management System (BRMS) solution. Using this framework users define rules that specify what action needs to be done when a particular condition is met.It extends and implements the Rete Pattern matching algorithm. In projects its usually used to define Business Rules. Business rules are composed of facts and conditional statements.

Problem : If you need to apply lot of rules based on udf its very complex to maintain since lot of 
if else but then and so on....


Solution : Drools came up with the solution drl file/decision table etc... where your logic will be written
business language in decision table(you can manage .drl files as well) can provide the rules dynamically and if they are in class path 
Required : Scala /Drools 6/Spark 2.3.1

Use case : While Streaming data from upstream systems, you can take simple decision on your data based on your rules.


Works like a standalone program in intellij to test easily... no need for any cluster.
