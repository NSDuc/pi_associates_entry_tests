import matplotlib.pyplot as plt
import pandas as pd

url = "https://raw.githubusercontent.com/fivethirtyeight/data/master/college-majors/recent-grads.csv"
df = pd.read_csv(url)
df.plot(x="Rank", y=["P25th", "Median", "P75th"])
plt.show()
