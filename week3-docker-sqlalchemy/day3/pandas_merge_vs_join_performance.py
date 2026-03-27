"""
Performance comparison of pandas merge and join operations on large DataFrames, 
demonstrating the importance of indexing for efficient data manipulation in pandas.

Based on: https://python.plainenglish.io/optimizing-pandas-merge-vs-join-for-faster-data-processing-3bfe8bb12aea


"""
import pandas as pd

# BASICS

## Pandas Merge

### Create two example DataFrames
df1 = pd.DataFrame({'ID': [1, 2, 3], 'Value_A': ['A', 'B', 'C']})
df2 = pd.DataFrame({'ID': [1, 2, 4], 'Value_B': ['X', 'Y', 'Z']})

# Merge based on the 'ID' column
merged_df = pd.merge(df1, df2, left_on='ID', right_on='ID', how='inner')
print(merged_df)

## Pandas Join

### Set the index before joining
df1_indexed = df1.set_index('ID')
df2_indexed = df2.set_index('ID')

### Join DataFrames based on their index
joined_df = df1_indexed.join(df2_indexed, how='inner')
print(joined_df)


# PERFORMANCE COMPARISON
print("\n\nPerformance Comparison of Merge vs Join on Large DataFrames\n")

import numpy as np
import time

def generate_2_large_dataframe(num_rows):
    return pd.DataFrame({
        'ID': np.arange(1, num_rows + 1),
        'Value_A': np.random.randint(1, 100, size=num_rows)
    }), pd.DataFrame({
        'ID': np.arange(1, num_rows + 1),
        'Value_B': np.random.randint(1, 100, size=num_rows)
    })

performance_comparison = []

## Merge

### Create two large DataFrames for the experiment
rows = 1_000_000  # Experiment with 1 million rows
df1, df2 = generate_2_large_dataframe(rows)
df1_indexed = df1.set_index('ID')
df2_indexed = df2.set_index('ID')

# print(df1.head())
# print(df2.head())
# print(df1_indexed.head())
# print(df2_indexed.head())

### Measure the time taken to perform the merge
start_time = time.time()
merged_df = pd.merge(df1, df2, left_on='ID', right_on='ID', how='inner')
end_time = time.time()
print(f"-- Merge completed in {end_time - start_time:.6f} seconds.")

## Join

### Measure the time taken to perform the join
start_time = time.time()
joined_df = df1_indexed.join(df2_indexed, how='inner')
end_time = time.time()
print(f"-- Join completed in {end_time - start_time:.6f} seconds.")



#for rows in [100_000, 500_000] + [1_000_000*i for i in range(1, 11)]:
for rows in [100_000, 500_000,
             1_000_000, 2_000_000, 5_000_000,
             10_000_000, 20_000_000, 
             50_000_000, 100_000_000,
]:
    df1, df2 = generate_2_large_dataframe(rows)

    df3, df4 = df1, df2
    #df3, df4 = generate_2_large_dataframe(rows)
    df1_indexed = df3.set_index('ID')
    df2_indexed = df4.set_index('ID')

    start_time = time.time()
    for _ in range(10):
        merged_df = pd.merge(df1, df2, left_on='ID', right_on='ID', how='inner')
    end_time = time.time()
    merge_time = (end_time - start_time) / 10.  # Average time over 10 iterations

    start_time = time.time()
    for _ in range(10):
        joined_df = df1_indexed.join(df2_indexed, how='inner')
    end_time = time.time()
    join_time = (end_time - start_time) / 10.  # Average time over 10 iterations

    performance_comparison.append({
        'rows': rows,
        'merge_time_sec': merge_time,
        'join_time_sec': join_time,
    })
    print(
        f"Rows: {rows:<10} - Merge Time: {merge_time:.6f} sec, Join Time: {join_time:.6f} sec, "
        f"Ratio (Join/Merge): {(join_time / merge_time)*100. - 100:.2f}%"
    )
print("\nPerformance Comparison Results:")
df_perf = pd.DataFrame(performance_comparison)
print(df_perf)


# Plotting the results

import matplotlib.pyplot as plt

## Data
rows = list(df_perf['rows'] / 1_000_000)  # in millions
merge_times = list(df_perf['merge_time_sec'])
join_times = list(df_perf['join_time_sec'])

## Plot
plt.plot(rows, merge_times, label='pd.merge()', marker='.')
plt.plot(rows, join_times, label='df.join()', marker='.')

## Labels and title
plt.xlabel('DataFrame Size (Millions of Rows)')
plt.ylabel('Time (Seconds)')
plt.title('Pandas Merge vs. Set_Index + Join: Performance Comparison')
plt.legend()

## Show plot
plt.show()
