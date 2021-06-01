from pyspark import SparkContext
from pyspark.sql.functions import length

class FastFD:
    def __init__(self, dataset, debug):
        self.debug: bool =  debug
        self.dataset: DataFrame = dataset

    def print_dataset(self):
        print(self.dataset.head())

    def stripped_partitions(self, col):
        '''
        Returns stripped partition for a certain column col
        '''
        # Create the partition set for col
        partition = set()

        # Fetch data and grab all distinct values
        col_data = self.dataset.select(self.dataset[col])
        col_data_values = [x[0] for x in col_data.distinct().collect()]

        # Loop over the values to see which rows are the same
        for value in col_data_values:
            same = set()
            for row in range(self.dataset.count()):
                if col_data.collect()[row][0] == value:
                    same.add(row)
            
            # Stripped partitions contain only sets with more then 1 entry
            if len(same) != 1:
                partition.add(frozenset(same))     

        if self.debug: print(f"Stripped partition for column {col}\t Partition set: {partition}")

        # Returns stripped partitions for col
        return partition

    def gen_diff_sets(self):
        resDS = set()
        strips = set()
        tmpAS = set()

        # Compute stripped partition
        for col in self.dataset.columns:
            partition = self.stripped_partitions(col)
            
            # Add every set to strips
            for part in partition:
                strips.add(part)

        if self.debug: print(f"Stripped partitions: {strips}")

        # Compute agreesets from stripped partitions
        # for strip in strips:
        #   for i in strip:
        #     for j in strip:
        #       if j > i:
        #         tmpAS.add((i, j))

        # Complement agree sets to get difference sets
        # for temp in tmpAS:
        #   resDS.add()
    
    def execute(self):
        '''
        Returns a list of all hard functional dependencies found in self.dataset using the FastFD algorithm
        '''
        print("Starting FastFD...")

        # Generate all difference sets
        self.gen_diff_sets()

        # For each column in R:
        # Compute D^A_r from D^r

        # If D^A_r == empty:
        # Output "empty --> A"

        # Else if empty \notin D^A_r then:
        # find_covers(...)
