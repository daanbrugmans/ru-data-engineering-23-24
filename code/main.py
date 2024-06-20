import datasets.untappd
import data_wrangler as dw

end_user_dataset = datasets.untappd.UntappdDataset()
data_wrangler = dw.DataWrangler(end_user_dataset)
data_wrangler.save_as_parquet("end_user_dataset")