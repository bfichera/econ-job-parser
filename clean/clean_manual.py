from pathlib import Path
import argparse

import pandas as pd

# pd.options.display.max_colwidth = 80
# pd.options.display.max_rows = 1000

parser = argparse.ArgumentParser(description='Clean manual data')
parser.add_argument('person', type=str)
parser.add_argument('csvfile', type=lambda s: Path(s))
parser.add_argument('output', type=lambda s: Path(s))
parser.add_argument('discarded', type=lambda s: Path(s))
parser.add_argument('academic', type=lambda s: Path(s))
parser.add_argument('verbose', type=lambda s: Path(s))
cfg = parser.parse_args()

manual_csv = Path(cfg.csvfile)


def load_manual(manual_csv):
    return pd.read_csv(manual_csv)


def filter_manual_default(manual):
    manual = manual.copy()
    manual['DISCARD'] = False
    return manual


def filter_manual(manual, person):
    manual = filter_manual_default(manual).copy()
    return manual


def format_manual(manual):
    new_manual = pd.DataFrame(manual)
    return (
        new_manual.loc[new_manual['DISCARD'] == False, :] # noqa
        .drop('DISCARD', axis=1)
        .sort_values(by=['Letter Submission Deadline Date', 'Department Name'])
        .reset_index(drop=True)
    )


if __name__ == '__main__':
    raw_manual = load_manual(manual_csv)
    person = cfg.person
    manual_person = filter_manual(raw_manual, person)
    verbose_person = manual_person.copy()
    verbose_person = verbose_person.loc[verbose_person['DISCARD'] == False, :] # noqa
    formatted_person = format_manual(manual_person)
    excel_person = formatted_person.drop(
        'ACADEMIC',
        axis=1,
    ).reset_index(drop=True)
    discarded_person = raw_manual.copy()
    discarded_person['ACADEMIC'] = manual_person['ACADEMIC']
    discarded_person = discarded_person.loc[manual_person['DISCARD'], :]
    excel_person_academic = (
        formatted_person.loc[formatted_person['ACADEMIC'], :]
        .drop('ACADEMIC', axis=1).reset_index(drop=True)
    )
    excel_person.to_csv(
        cfg.output / f'manual_{person}.csv',
        index=False,
    )
    discarded_person.to_csv(
        cfg.discarded / f'{person}_discarded.csv',
        index=False,
    )
    excel_person_academic.to_csv(
        cfg.academic / f'manual_{person}.csv',
        index=False,
    )
    verbose_person.to_csv(
        cfg.verbose / f'verbose_{person}.csv',
        index=False,
    )
