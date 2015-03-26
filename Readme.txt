Group 16 Readme.txt

Assumptions:
- The data starts from 0th row and 0th column.
- The schema header is not included in the input data file.
- For Spatial Join query, 

Assuming the configaration is done and the workers are up and running, the following commands can be used to run the respective operations:


Polygon Union
The following arguments must be provided from command line.
Argument 1: Master Url. Example- "spark://10.0.0.4:7077"
Argument 2: Path to Input file. Example- "hdfs://master:54310/content/PolygonUnionTestData.csv"
Argument 3: Path to Partial output directory after Mapping is complete. Example- Path to Partial output directory after Mapping is complete. Example- "hdfs://master:54310/content/PolygonUnionPartial"
Argument 4: Path to Final Results directory. Example- "hdfs://master:54310/content/PolygonUnionResults"

Therefore, please run the following command :
./spark-submit --class <GroupId>.<ArtifactID>.<ClassName> --jars <Path to JTS jar file> --master <masterURL> <Path to SNAPSHOT jar file> <Argument1> <Argument2> <Argument3> <Argument4>

Example:
./spark-submit --class geospatial.operations.PolygonUnion --jars /home/group16/Downloads/operations/jts-1.13.jar --master spark://ubuntu:7077 /home/group16/Downloads/operations/target/operations-0.0.1-SNAPSHOT.jar "spark://10.0.0.4:7077" "hdfs://master:54310/content/PolygonUnionTestData.csv" "hdfs://master:54310/content/PolygonUnionPartial" "hdfs://master:54310/content/PolygonUnionResults"





Convex Hull
The following arguments must be provided from command line.
Argument 1: Master Url. Example- "spark://10.0.0.4:7077"
Argument 2: Path to Input file. Example- "hdfs://master:54310/content/ConvexHullTestData.csv"
Argument 3: Path to Partial output directory after Mapping is complete. Example- "hdfs://master:54310/content/ConvexHullPartialResults"
Argument 4: Path to Final Results directory. Example- "hdfs://master:54310/content/ConvexHullResults"

Therefore, please run the following command :
./spark-submit --class <GroupId>.<ArtifactID>.<ClassName> --jars <Path to JTS jar file> --master <masterURL> <Path to SNAPSHOT jar file> <Argument1> <Argument2> <Argument3> <Argument4>

Example:
./spark-submit --class geospatial.operations.ConvexHull --jars /home/group16/Downloads/operations/jts-1.13.jar --master spark://ubuntu:7077 /home/group16/Downloads/operations/target/operations-0.0.1-SNAPSHOT.jar "spark://10.0.0.4:7077" "hdfs://master:54310/content/ConvexHullTestData.csv" "hdfs://master:54310/content/ConvexHullPartialResults" "hdfs://master:54310/content/ConvexHullResults"






Farthest Pair
The following arguments must be provided from command line.
Argument 1: Master Url. Example- "spark://10.0.0.4:7077"
Argument 2: Path to Input file. Example- "hdfs://master:54310/content/FarthestPairandClosestPairTestData.csv"
Argument 3: Path to Partial output directory after Mapping is complete. Example- "hdfs://master:54310/content/FarthestPairPartial"
Argument 4: Path to Final Results directory. Example- "hdfs://master:54310/content/FarthestPairFinalResults"

Therefore, please run the following command :
./spark-submit --class <GroupId>.<ArtifactID>.<ClassName> --jars <Path to JTS jar file> --master <masterURL> <Path to SNAPSHOT jar file> <Argument1> <Argument2> <Argument3> <Argument4>

Example:
./spark-submit --class geospatial.operations.FarthestPair --jars /home/group16/Downloads/operations/jts-1.13.jar --master spark://ubuntu:7077 /home/group16/Downloads/operations/target/operations-0.0.1-SNAPSHOT.jar "spark://10.0.0.4:7077" "hdfs://master:54310/content/FarthestPairandClosestPairTestData.csv" "hdfs://master:54310/content/FarthestPairPartial" "hdfs://master:54310/content/FarthestPairFinalResults"






Closest Pair
The following arguments must be provided from command line.
Argument 1: Master Url. Example- "spark://10.0.0.4:7077"
Argument 2: Path to Input file. Example- "hdfs://master:54310/content/FarthestPairandClosestPairTestData.csv"
Argument 3: Path to Partial output directory after Mapping is complete. Example- "hdfs://master:54310/content/ClosestPairPartial"
Argument 4: Path to Final Results directory. Example- "hdfs://master:54310/content/ClosestPairResults"

Therefore, please run the following command :
./spark-submit --class <GroupId>.<ArtifactID>.<ClassName> --jars <Path to JTS jar file> --master <masterURL> <Path to SNAPSHOT jar file> <Argument1> <Argument2> <Argument3> <Argument4>

Example:
./spark-submit --class geospatial.operations.ClosesPair --jars /home/group16/Downloads/operations/jts-1.13.jar --master spark://ubuntu:7077 /home/group16/Downloads/operations/target/operations-0.0.1-SNAPSHOT.jar "spark://10.0.0.4:7077" "hdfs://master:54310/content/FarthestPairandClosestPairTestData.csv" "hdfs://master:54310/content/ClosestPairPartial" "hdfs://master:54310/content/ClosestPairResults"






Spatial Join
The following arguments must be provided from command line.
Argument 1: Master Url. Example- "spark://10.0.0.4:7077"
Argument 2: Path to Broadcast file(window Query). Example- "hdfs://master:54310/content/file1.csv"
Argument 3: Path to RDD file(Polygons). Example- "hdfs://master:54310/content/file2.csv"
Argument 4: Path to Final Results directory. Example- "hdfs://master:54310/content/JoinQueryResults"


Therefore, please run the following command :
./spark-submit --class <GroupId>.<ArtifactID>.<ClassName> --master <masterURL> <Path to SNAPSHOT jar file> <Argument1> <Argument2> <Argument3> <Argument4>

Example:
./spark-submit --class geospatial.operations.SpatialJoin --master spark://ubuntu:7077 /home/group16/Downloads/operations/target/operations-0.0.1-SNAPSHOT.jar "spark://10.0.0.4:7077" "hdfs://master:54310/content/file1.csv" "hdfs://master:54310/content/file2.csv" "hdfs://master:54310/content/JoinQueryResults"






Spatial Range
The following arguments must be provided from command line.
Argument 1: Master Url. Example- "spark://10.0.0.4:7077"
Argument 2: Path to Broadcast file(window Query). Example- "hdfs://master:54310/content/file1.csv"
Argument 3: Path to RDD file(Polygons). Example- "hdfs://master:54310/content/file2.csv"
Argument 4: Path to Final Results directory. Example- "hdfs://master:54310/content/RangeQueryResults"


Therefore, please run the following command :
./spark-submit --class <GroupId>.<ArtifactID>.<ClassName> --master <masterURL> <Path to SNAPSHOT jar file> <Argument1> <Argument2> <Argument3> <Argument4>

Example:
./spark-submit --class geospatial.operations.SpatialRange --master spark://ubuntu:7077 /home/group16/Downloads/operations/target/operations-0.0.1-SNAPSHOT.jar "spark://10.0.0.4:7077" "hdfs://master:54310/content/RangeQueryTestData.csv" "hdfs://master:54310/content/rectangles.csv" "hdfs://master:54310/content/RangeQueryResults"



