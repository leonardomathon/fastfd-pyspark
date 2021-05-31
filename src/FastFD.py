from pyspark import SparkContext


class FastFD:
    def __init__(self, dataset):
        self.dataset: DataFrame = dataset

    def print_dataset(self):
        print(self.dataset.head())

    def gen_diff_sets(self):
        # TODO
        pass

    def loop_columns(self):
        for col1 in self.dataset.columns:
            for col2 in self.dataset.columns:
                if (col1 != col2):
                    print(f"Tuple: \t ({col1}, {col2})")

    
    def execute(self):
        '''
        Returns a list of all hard functional dependencies found in self.dataset using the FastFD algorithm
        '''
        print("executing")
        # gen_diff_sets(R, r)

        # For each column in R:
        # Compute D^A_r from D^r

        # If D^A_r == empty:
        # Output "empty --> A"

        # Else if empty \notin D^A_r then:
        # find_covers(...)
