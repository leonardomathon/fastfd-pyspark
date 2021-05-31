class FastFD:
    def __init__(self, dataset):
        self.dataset = dataset

    def print_dataset(self):
        print(dataset)

    def gen_diff_sets(self, R, r):
        # TODO

    def execute(self):
        # gen_diff_sets(R, r)

        # For each column in R:
            # Compute D^A_r from D^r

            # If D^A_r == empty:
                # Output "empty --> A"
            
            # Else if empty \notin D^A_r then:
                # find_covers(...)
