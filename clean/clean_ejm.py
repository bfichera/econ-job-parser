from pathlib import Path
import re
import argparse
import time
import random
import logging
import tomllib

from tqdm import tqdm
import requests
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from dateparser import parse
import numpy as np
import pandas as pd
from datefinder import find_dates

logger = logging.getLogger(__file__)
logging.basicConfig(level=logging.WARNING)

parser = argparse.ArgumentParser(description='Clean EJM data')
parser.add_argument('csvfile', type=lambda s: Path(s))
parser.add_argument('output', type=lambda s: Path(s))
parser.add_argument('discarded', type=lambda s: Path(s))
parser.add_argument('academic', type=lambda s: Path(s))
parser.add_argument('verbose', type=lambda s: Path(s))
parser.add_argument('--getlinks', action='store_true')
parser.add_argument('--tries', type=int, default=1)
cfg = parser.parse_args()

ejm_csv = Path(cfg.csvfile)

with open(Path(__file__).parent / '..' / 'config' / 'exclude.toml',
          'rb') as fh:
    exclude = tomllib.load(fh)


def extract_ejmcat_codes(ejmcat_entry):
    prog = re.compile(r'(?:,\s*|;\s*)')
    ejmcat_strs = prog.split(ejmcat_entry)
    result = []
    for ejmcat_str in ejmcat_strs:
        result.append(ejmcat_str.lower())
    return result


def load_ejm(ejm_csv):
    return pd.read_csv(ejm_csv, skiprows=1)


def ejm_earliest_date(row, lowerbound, upperbound):

    def find_best(s):
        if pd.isna(s):
            return parse('9999-01-01')
        clean_matches = []
        clean_sources = []
        result = list(find_dates(s, source=True))
        sources = [r[1] for r in result]
        for source in sources:
            check = list(find_dates(source, strict=True))
            if check:
                for c in check:
                    clean_matches.append(c)
                    continue
            else:
                check2 = list(
                    find_dates(source + ' 2024', strict=True, source=True)
                )
                for c in check2:
                    clean_matches.append(c[0])
                    clean_sources.append(c[1])
                    continue
        clean_matches = [c.replace(tzinfo=None) for c in clean_matches]
        for c in sorted(clean_matches):
            if c > lowerbound and c < upperbound:
                return c
        if clean_matches:
            return min(clean_matches)
        return parse('9999-02-02')

    def parse_(s):
        try:
            return parse(s)
        except TypeError:
            return parse('9999-02-02')

    earliest_ad_text_d = find_best(row['Ad text (in markdown format)'])
    earliest_official_d = min(
        [
            parse_(s)
            for s in [row['Date closes'], row['Target date'], row['Deadline']]
        ]
    )
    d = min([earliest_ad_text_d, earliest_official_d])
    return f'{d.year}-{d.month:02d}-{d.day:02d}'


def ejm_is_academic(row):
    section = row['Types'].lower()
    return not ('nonacademic' in section or 'non-academic' in section)


def ejm_contains_desired_ejmcat_code(row):
    prog = re.compile('any.field')
    saysanyfield = prog.search(row['Ad text (in markdown format)'])
    result = row['EJMCAT_Codes'] == [] or any(
        ejmcat_code not in exclude['ejmcats']
        for ejmcat_code in row['EJMCAT_Codes']
    )
    return result or saysanyfield


def ejm_is_postdoc(row):
    title = row['Types'].lower()
    return 'postdoc' in title or 'post-doc' in title or 'post doc' in title


def ejm_is_full_prof(row):
    title = row['Types'].lower()
    return 'full' in title


def ejm_is_associate_prof(row):
    title = row['Types'].lower()
    return 'associate' in title


def ejm_is_assistant_prof(row):
    title = row['Types'].lower()
    return 'assistant' in title


def ejm_is_lecturer(row):
    title = row['Types'].lower()
    return 'lect' in title


def ejm_is_visiting(row):
    title = row['Types'].lower()
    return 'visit' in title


def countries(row):
    try:
        return [row['Country'].upper()]
    except AttributeError:
        if np.isnan(row['Country']):
            return ['NO COUNTRY']
        else:
            raise AttributeError


def ejm_contains_bad_country(row):
    for c in row['COUNTRIES']:
        if c in exclude['countries']:
            return True
    return False


def ejm_application_instructions(row, session, bar=None):
    try:
        if not cfg.getlinks:
            return 'ERRORNOLINKDIVIDERERRORNOLINK'
        for i in range(cfg.tries):
            try:
                url = row['URL']
                logging.info(f'Asking for data from\n{url}')
                response = session.get(url)
                soup = BeautifulSoup(response.content, 'html.parser')
                div = soup.find(
                    'div',
                    class_='panel-heading',
                    string='Application procedure',
                )
                p = div.parent.find('div', class_='panel-body')
                text = p.get_text(strip=True)
                link = p.find('a')
                if link:
                    result = 'DIVIDER'.join([text, link['href']])
                else:
                    result = 'DIVIDER'.join([text, 'ERRORNOLINK'])
                return result
            except Exception as e:
                logger.warning(f'Error for\n{url} on try {i+1}:\n{e}')
            finally:
                time.sleep(random.randint(2, 4))
        return 'ERRORNOLINKDIVIDERERRORNOLINK'
    finally:
        if bar is not None:
            bar.update()


def ejm_parse_instructions(row, instructions, e):
    id_ = row['Id']
    text, link = (
        instructions.loc[instructions['ID'] == id_, :]['result'].item()
    ).split('DIVIDER')
    if text == 'ERRORNOLINK':
        text = None
    if link == 'ERRORNOLINK':
        link = None
    return (text, link)[e]


def filter_ejm(ejm):
    ejm = filter_ejm_default(ejm).copy()
    ejm['BAD COUNTRY'] = ejm.apply(
        lambda row: (ejm_contains_bad_country(row)),
        axis=1,
    )
    ejm['BAD EJMCAT CODES'] = ejm.apply(
        lambda row: (not ejm_contains_desired_ejmcat_code(row)),
        axis=1,
    )
    ejm['ACADEMIC'] = ejm.apply(
        lambda row: ejm_is_academic(row),
        axis=1,
    )
    ejm['POSTDOC'] = ejm.apply(
        lambda row: (ejm_is_postdoc(row)),
        axis=1,
    )
    ejm['LECTURER'] = ejm.apply(
        lambda row: (ejm_is_lecturer(row)),
        axis=1,
    )
    ejm['ASSISTANT PROF'] = ejm.apply(
        lambda row: (ejm_is_assistant_prof(row)),
        axis=1,
    )
    ejm['ASSOCIATE PROF'] = ejm.apply(
        lambda row: (ejm_is_associate_prof(row)),
        axis=1,
    )
    ejm['FULL PROF'] = ejm.apply(
        lambda row: (ejm_is_full_prof(row)),
        axis=1,
    )
    ejm['VISITING'] = ejm.apply(
        lambda row: ejm_is_visiting(row),
        axis=1,
    )
    ejm['DISCARD'] = ejm.apply(
        lambda row: (
            row['BAD COUNTRY'] or (
                row['ACADEMIC'] and (
                    row['BAD EJMCAT CODES'] or row['BAD COUNTRY'] or
                    row['VISITING'] or (
                        (
                            row['FULL PROF'] or row['ASSOCIATE PROF'] or row[
                                'LECTURER']
                        ) and not (row['ASSISTANT PROF'] or row['POSTDOC'])
                    )
                )
            )
        ),
        axis=1,
    )
    ejm['EARLIEST DATE'] = ejm.apply(
        ejm_earliest_date,
        axis=1,
        lowerbound=parse('10/01/2024'),
        upperbound=parse('12/01/2024'),
    )
    with requests.Session() as s:
        instructions = pd.DataFrame()
        loginurl = 'https://econjobmarket.org/login'
        loginform = BeautifulSoup(
            s.get(loginurl).content,
            'html.parser',
        )
        token = loginform.find(
            'meta',
            attrs={'name': 'csrf-token'},
        )['content']
        with open(Path(__file__).parent / '../config/ejm_login.toml',
                  'rb') as fh:
            ejm_login = tomllib.load(fh)
            payload = {
                '_token': token,
                'email': ejm_login['email'],
                'password': ejm_login['password'],
            }
        s.post(loginurl, data=payload)
        bar = tqdm(total=len(ejm))
        instructions['result'] = ejm.apply(
            lambda row: ejm_application_instructions(row, s, bar=bar),
            axis=1,
        )
    instructions['ID'] = ejm['Id']
    ejm['APPLICATION INSTRUCTIONS'] = ejm.apply(
        lambda row: ejm_parse_instructions(row, instructions, 0),
        axis=1,
    )
    ejm['APPLICATION LINK'] = ejm.apply(
        lambda row: ejm_parse_instructions(row, instructions, 1),
        axis=1,
    )
    ejm['SUBMISSION TYPE'] = ejm.apply(
        format_application_link,
        axis=1,
    )
    return ejm


def format_application_link(row):
    link = row['APPLICATION LINK']
    if pd.isna(link):
        return None
    netloc = urlparse(link).netloc
    if 'interfolio' in netloc:
        return 'Interfolio Submission'
    if 'econjobmarket' in netloc:
        return 'EconJobMarket Submission'
    if 'aea' in netloc:
        return 'AEA\'s JOE Submission'
    return 'Direct email to be sent by Admin'


def filter_ejm_default(ejm):
    ejm = ejm.copy()
    ejm['COUNTRIES'] = ejm.apply(
        lambda row: countries(row),
        axis=1,
    )
    ejm['EJMCAT_Codes'] = ejm.apply(
        lambda row: extract_ejmcat_codes(row['Categories']),
        axis=1,
    )
    ejm['DISCARD'] = False
    return ejm


def format_ejm(ejm):
    new_ejm = pd.DataFrame(
        {
            'Submission Type': ejm['SUBMISSION TYPE'],
            'Letter Submission Deadline Date': ejm['EARLIEST DATE'],
            'Application Status': pd.NA,
            'Institution or Organization Name': ejm['Institution'],
            'Department Name': ejm['Department'],
            'Job ID #': ejm['Id'],
            'Job Title': ejm['Types'],
            'Additional Instructions': pd.NA,
            'Ad Webpage Link': ejm['URL'],
            'DISCARD': ejm['DISCARD'],
            'ACADEMIC': ejm['ACADEMIC'],
        },
    )
    return (
        new_ejm[new_ejm['DISCARD'] == False]  # noqa
        .drop('DISCARD', axis=1).sort_values(
            by=['Letter Submission Deadline Date', 'Department Name']
        ).reset_index(drop=True)
    )


if __name__ == '__main__':
    raw_ejm = load_ejm(ejm_csv)
    ejm = filter_ejm(raw_ejm)
    verbose = ejm.copy()
    verbosn = verbose[verbose['DISCARD'] == False]  # noqa
    formatted = format_ejm(ejm)
    excel = formatted.drop(
        'ACADEMIC',
        axis=1,
    ).reset_index(drop=True)
    discarded = raw_ejm.copy()
    discarded['ACADEMIC'] = ejm['ACADEMIC']
    discarded = discarded[ejm['DISCARD']]
    excel_academic = (
        formatted[formatted['ACADEMIC']].drop('ACADEMIC',
                                              axis=1).reset_index(drop=True)
    )
    excel.to_csv(
        cfg.output / 'ejm.csv',
        index=False,
    )
    discarded.to_csv(
        cfg.discarded / 'discarded.csv',
        index=False,
    )
    excel_academic.to_csv(
        cfg.academic / 'ejm.csv',
        index=False,
    )
    verbose.to_csv(
        cfg.verbose / 'verbose.csv',
        index=False,
    )
