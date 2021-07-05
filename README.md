# bstl-scala-preview


-   Your system should have Scala installed
-   For build of project you should use SBT
-   Details about installing SBT can be found [here](https://www.scala-sbt.org/1.0/docs/Installing-sbt-on-Linux.html 'here')

# Build

SBT has several steps just like Maven:

| Step     | Description                                         |
| -------- | --------------------------------------------------- |
| clean    | delete all previously built results               |
| compile  | compile code                                        |
| assembly | Assemble code into single jar located in bin folder |

So typical build would look like:

```
dev-box> bst-scala

dev-box> sbt clean

dev-box> sbt compile

dev-box> sbt assembly

```

By default sbt builds for hadoop 3.1, but with a switch we can compile for 2.6

```
dev-box> sbt -Dhdp=2.6 compile assembly
```

First step would took several minutes at first run, because it will download all required packages, but than it would be faster

