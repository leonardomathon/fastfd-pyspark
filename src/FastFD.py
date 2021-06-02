from typing import List
from pyspark import SparkContext
from pyspark.sql.functions import length

class FastFD:
    def __init__(self, dataset, debug, logger):
        self.debug: bool =  debug
        self.logger = logger
        self.dataset: DataFrame = dataset
        self.fds: list = []

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

        if self.debug: self.logger.info(f"Stripped partition for column {col}\t Partition set: {partition}")

        # Returns stripped partitions for col
        return partition

    def find_match_set(self, t1, t2):
        '''
        Find the columns where t1 and t2 agree on
        '''
        agree = set()
        
        # Loop over columns
        for col in self.dataset.columns:
            col_data = self.dataset.select(self.dataset[col])
            # See if t1 and t2 agree and add them
            if col_data.collect()[t1][0] == col_data.collect()[t2][0]:
                agree.add(col)           
        
        return agree

    def complement_set(self, agree_set):
        '''
        Computes the complement w.r.t agree_set
        '''
        col_set = frozenset(self.dataset.columns)
        return col_set - agree_set


    def gen_diff_sets(self):
        '''
        Computes difference sets Dr from r and R
        '''
        resDS = set()
        strips = []
        tmpAS = set()

        # Compute stripped partition
        for col in self.dataset.columns:
            strips.append(self.stripped_partitions(col))

        if self.debug: self.logger.info(f"Stripped partitions done\n")
        if self.debug: self.logger.info(f"Stripped partitions: {strips}\n")

        # Compute agreesets from stripped partitions
        for attribute in strips:
            for partition in attribute:
                # Loop over all tuples in a stripped partition
                for t1 in partition:
                    for t2 in partition:
                        if t2 > t1:
                            tmpAS.add(frozenset(self.find_match_set(t1, t2)))

        if self.debug: self.logger.info(f"Agree set: {tmpAS}\n")

        # Complement agree sets to get difference sets
        for temp in tmpAS:
            resDS.add(self.complement_set(temp))

        if self.debug: self.logger.info(f"Difference set: {resDS}\n")
        
        # Return the difference sets
        return resDS

    def gen_min_diff_sets(self, diff_sets, col):
        '''
        Computes minimal difference set Dr_a from Dr
        '''
        sub_minimal = set()
        col_set = frozenset(col)
      
        # Sub minimal difference sets for specified column
        for diff_set in diff_sets:
            if col in diff_set:
                temp_set = frozenset()
                temp_set = diff_set - col_set
                if len(temp_set) != 0:
                    sub_minimal.add(temp_set) 

        minimal = sub_minimal.copy()

        # Minimal difference sets for specified column
        for p1 in sub_minimal:
            for p2 in sub_minimal:
                # Remove differences that have a subset
                if (p1 != p2) and (p1.issubset(p2)):
                    if p2 in  minimal:
                        minimal.remove(p2)

        if self.debug: self.logger.info(f"Minimal difference set for {col} is: {minimal}\n")

        # Return the minimal difference set
        return minimal
  
    def find_covers(self):
        pass

    def execute(self):
        '''
        Returns a list of all hard functional dependencies found in self.dataset using the FastFD algorithm
        '''
        self.logger.info("Starting FastFD...\n")

        # Generate all difference sets
        diff_sets = self.gen_diff_sets()

        # Generate all minimal difference sets
        for col in self.dataset.columns:
            min_diff_set = self.gen_min_diff_sets(diff_sets, col)

            if len(min_diff_set) != 0:
                pass


        # For each column in R:
        # Compute D^A_r from D^r
        #for col in self.dataset.columns:

        # If D^A_r == empty:
        # Output "empty --> A"

        # Else if empty \notin D^A_r then:
        # find_covers(...)
