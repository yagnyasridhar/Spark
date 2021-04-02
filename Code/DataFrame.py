import pyspark
from pyspark import SparkContext, SQLContext

sc = SparkContext.getOrCreate()
sql = SQLContext(sc)

Student = sql.createDataFrame([  
('009001', 'Anuj', '70%', 'B.tech(cs)'),  
('009002', 'Sachin', '80%', 'B.tech(cs)'),  
('008005', 'Yogesh', '94%', 'MCA'),  
('007014', 'Ananya', '98%', 'MCA')],  
['Roll_Num', 'Name', 'Percentage','Department']  
)  
Student.show()