name := "GDELT_Dataset"

version := "1.0"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.3.32" 
