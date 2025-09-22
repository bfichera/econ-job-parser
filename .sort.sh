#!/usr/bin/env python3

import pandas as pd

file = 'to_admin.csv'
d = pd.read_csv(file)
d.sort_values(
    by=[
        'Letter Submission Deadline Date',
        'Application Status',
        'Institution or Organization Name',
        'Department Name',
        'Job ID #',
        'Job Title',
        'Additional Instructions',
        'Ad Webpage Link',
    ],
    inplace=True,
)
with open(file, 'w') as fh:
    d.to_csv(fh, index=False)
