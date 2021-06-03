from typing import List
from pyspark import SparkContext
from pyspark.sql.functions import length
from FD import FD

class FastFD:
    def __init__(self, dataset, debug, logger):
        self.debug: bool =  debug
        self.logger = logger
        self.dataset: DataFrame = dataset
        self.fds: list = []
        self.temp_covers = set()

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
        Computes difference sets D_r from r and R
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
        Computes minimal difference set D_r^a from D_r
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
      
    def find_ordering(self, elements, diff_set):
        # Initialize the list keeping track how many difference sets each remaining attribute covers
        count = []

        # Iterate over all remaining attributes of the ordering
        for index, col in enumerate(elements):
            col_set = frozenset(col)
            count.append([col, 0])
            for diff in diff_set: 
                # Increment the counter for the current attribute
                if col_set.issubset(diff):
                    count[index][1] = count[index][1] + 1      
        
        # Sort counters in the ordering based on descending counters      
        sorted_ordering = sorted(count, key=lambda x: -x[1])
        # Sort the attributes in the ordering based on the corresponding counters
        ordering = [i[0] for i in sorted_ordering]

        # Return the lexographical ordering
        return ordering

    def find_covers(self, col, DS_original, DS_remaining, path, order):
        print(f"Start find covers for {col}\n")

        # Difference sets left to cover, but no attributes left to unfold
        if (len(order) == 0) and (len(DS_remaining) > 0):
            print(f"First loop, no FD's here\n")
            return
        # No difference sets left to cover
        elif (len(DS_remaining) == 0):
            print(f"Enter second loop\n")
            temp_path = frozenset(path)
            # If the FD is not be minimal return 
            for cover in self.temp_covers:
                if cover.issubset(temp_path):
                    print(f"Second loop, subset return\n")
                    return
            # Otherwise, output the subset built along the path to the current leaf as a LHS for an FD for the current attribute
            self.temp_covers.add(frozenset(path))
            print(f"Second loop, fd added\n")
        # Initialize recursion tree
        else:
            # Iterate over all remaining attributes in the ordering
            for col in order:
                DS_remaining = set()
                temp_col = frozenset(col)

                # Compute the difference sets that are not yet covered by the attribute
                for diff in DS_original:
                    if not temp_col.issubset(diff):
                        print(f"{temp_col} is not a subset of {diff}\n")
                        DS_remaining.add(diff)

                # Compute the lexographical ordering based on the remaining difference sets
                index_col = order.index(col)
                temp_order = order.copy()
                for element in order:
                    if order.index(element) <= index_col:
                        temp_order.remove(element)
                new_order = self.find_ordering(temp_order, DS_remaining)

                # Update the path
                new_path = path.copy()
                new_path.append(col)
                print(f"Path is {path}\n")

                # Recursive iteration
                self.find_covers(col, DS_remaining, DS_remaining, new_path, new_order)

        # Return the lhs of all fds for the current attribute
        return self.temp_covers

    def print_fds(self):
        '''
        Prints the list of FD's
        '''
        for fd in self.fds:
            print(fd)

    def execute(self):
        '''
        Returns a list of all hard functional dependencies found in self.dataset using the FastFD algorithm
        '''
        self.logger.info("Starting FastFD...\n")

        # Generate all difference sets
        diff_sets = self.gen_diff_sets()


        for col in self.dataset.columns:
            # Clear the set containing covers for the previous attribute
            self.temp_covers = set()
            # Generate all minimum difference sets for the current attribute
            min_diff_set = self.gen_min_diff_sets(diff_sets, col)

            # If the minimal difference set is empty, there are no fd's for the current attribute
            if len(min_diff_set) == 0:
                lhs = frozenset('∅')
                rhs = frozenset(col)
                self.fds.append(FD(lhs, rhs))
            else: 
                # Initialize the path
                path = []
                # Initialize the list of elements necessary to compute the lexographical order
                elements = []

                # Create a list of all columns except the current one
                for attr in self.dataset.columns:
                    if col != attr:
                        elements.append(attr)

                # Create the lexographical order
                order = self.find_ordering(elements, min_diff_set)
                print(f"The order for {col} is: {order}\n") 
                
                # Set containing the lhs of all fds with the current attribute on the rhs
                covers_attr = self.find_covers(col, min_diff_set, min_diff_set, path, order)

                # Add all fds with the current attribute on the rhs to the global fd list
                for cover in covers_attr:
                    lhs = cover
                    rhs = frozenset(col)
                    self.fds.append(FD(lhs, rhs))

        # Print all fds
        self.print_fds()

