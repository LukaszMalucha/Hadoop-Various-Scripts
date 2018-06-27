from pyspark.sql import SQLContext, Row, HiveContext

## Option #1
hiveContext = HiveContext(sc)

### Option #2
inputData = spark.read.json(dataFile)

inputData.createOrReplaceTempView("myStructuredData")

myResultDataFrame = hiveContext.sql("""SELECT foo FROM bar ORDER BY foobar""")


### Other

myResultDataFrame.show()
myResultDataFrame.select("someFieldName")
myResultDataFrame.filter(myResultDataFrame("someFieldName" > 200))
myResultDataFrame.groupBy(myResultDataFrame("someFieldName")).mean()
myResultDataFrame.rdd().map(mapperFunction)


### UDF

from pyspark.sql.types import IntegerType
hiveCtx.registerFunction("square", lambda x: x*x, IntegerType())
df = hiveCtx.sql("""SELECT foo FROM bar ORDER BY foobar#""")