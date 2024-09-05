import numpy as np
import pandas as pd

# ANSI escape codes for colors
class colors:
    RESET = '\033[0m'
    BOLD = '\033[1m'
    RED = '\033[31m'
    GREEN = '\033[32m'
    YELLOW = '\033[33m'
    BLUE = '\033[34m'
    PURPLE = '\033[35m'
    CYAN = '\033[36m'
    WHITE = '\033[37m'
    UNDERLINE = '\033[4m'

blue =colors.BLUE
green = colors.GREEN
reset = colors.RESET

data1 = {'name': ['Azyyati', 'Huda', 'Thacha', 'Zamza', 'Sarah'],
         'cat': [15, 1, 1, 1, np.NaN]
         }

data2 = {'name': ['Hakimul', 'Zamza', 'Anwar', 'Faizul'],
         'child': [2, 2, 1, 4]}

df1 = pd.DataFrame(data1)
df2 = pd.DataFrame(data2)



# Merge dataframe, how=left
merge_left = pd.merge(df1, df2, how='left', left_on='name', right_on='name')
merge_right = pd.merge(df1, df2, how='right', left_on='name', right_on='name')
merge_inner = pd.merge(df1, df2, left_on='name', right_on='name')
merge_outer =pd.merge(df1, df2, how='outer', left_on='name', right_on='name')
merge_cross = pd.merge(df1, df2, how='cross')

print(f'{blue}Declare Dataframe 1\n{green}df1 = pd.DataFrame(data1){reset}\n{df1}\n')
print(f'{blue}Declare Dataframe 2\n{green}df2 = pd.DataFrame(data2){reset}\n{df2}\n')
print(f"{blue}Merge df1 and df2, how=left\n{green}merge_left = pd.merge(df1, df2, how='left', left_on='name', right_on='name'){reset}\n{merge_left}\n")
print(f"{blue}Merge df1 and df2, how=right\n{green}merge_right = pd.merge(df1, df2, how='right', left_on='name', right_on='name'){reset}\n{merge_right}\n")
print(f"{blue}Merge df1 and df2 by default method is 'inner'\n{green}merge_inner = pd.merge(df1, df2, left_on='name', right_on='name'){reset}\n{merge_inner}\n")
print(f"{blue}Merge df1 and df2 method 'outer'\n{green}merge_outer =pd.merge(df1, df2, how='outer', left_on='name', right_on='name'){reset}\n{merge_outer}\n")
print(f"{blue}Merge df1 and df2 method 'cross'\n{green}merge_outer =pd.merge(df1, df2, how='cross'){reset}\n{merge_cross}\n")
