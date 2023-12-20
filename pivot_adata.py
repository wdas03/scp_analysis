import dask.dataframe as dd

if __name__ == "__main__":
    print("Loading adata_train...")
    adata_train = dd.read_parquet('data/adata_train.parquet')

    print("Loading adata_obs...")
    adata_obs = dd.read_csv('data/adata_obs_meta.csv')

    # Step 1: Aggregate adata_train using groupby and unstack
    print("Aggregating adata_train...")
    grouped = adata_train.groupby(['obs_id', 'gene'])
    adata_train_aggregated = grouped['normalized_count'].sum().unstack()

    # Step 2: Ensure Consistency of Indices
    print("Aligning adata_obs...")
    adata_obs_meta_sorted = adata_obs.set_index('obs_id').reindex(adata_train_aggregated.index)

    print("Storing aggregated data...")
    adata_train_aggregated.to_csv("data/adata_train_aggregated.csv")
    adata_obs_meta_sorted.to_csv("data/adata_obs_meta_sorted.csv")
