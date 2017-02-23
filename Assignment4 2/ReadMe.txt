Readme about EQuijoin in hadoop

## Mapper
The mapper function takes in the input record and extracts second field as the key and assign tuples as the values for all the record.
This is realized in the code using the split function, from which second field is extracted.
The output key,value pairs are written into the context.

## Reducer
A string containing entire records are built by iterating through all the records using string builder, separated by semicolon.
This record is further processed into single records by string split on semicolon.
Finally using 2 loops , outer loop running from 0 to size-1 and inner loop running from 1 to size, joins are performed.
It compares if 2 records are from the same table , if not they are appended as unique records into the output record field and appended to the context.

## Main function 
Inside the main class new configuration are declared , which are assigned as a new job .
For the job, corresponding main class , map class and reduce class are defined using SetJarClass, SetMapperclass and SetReducerclass are defined.
The corresponding input key value pairs are also extracted using this.

# Command used to run Hadoop
sudo -u newhduser /usr/local/hadoop/bin/hadoop jar /home/thamizh/workspace/equijoin/target/equijoin.jar equijoin /newhduser/data/sample.txt /newhduser/output

name of the main class "equijoin"
jar file name "equijoin.jar"
