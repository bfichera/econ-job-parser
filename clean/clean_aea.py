from pathlib import Path
import re
import argparse
import requests
import logging
import time
import random
import tomllib
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from tqdm import tqdm
import pandas as pd
from dateparser import parse
from datefinder import find_dates

logger = logging.getLogger(__file__)
logging.basicConfig(level=logging.WARNING)

parser = argparse.ArgumentParser(description='Clean AEA data')
parser.add_argument('csvfile', type=lambda s: Path(s))
parser.add_argument('output', type=lambda s: Path(s))
parser.add_argument('discarded', type=lambda s: Path(s))
parser.add_argument('academic', type=lambda s: Path(s))
parser.add_argument('verbose', type=lambda s: Path(s))
parser.add_argument('--getlinks', action='store_true')
parser.add_argument('--tries', type=int, default=1)
cfg = parser.parse_args()

aea_csv = Path(cfg.csvfile)

with open(Path(__file__).parent / '..' / 'config' / 'exclude.toml',
          'rb') as fh:
    exclude = tomllib.load(fh)


def extract_jel_codes(jel_entry):
    jel_strs = jel_entry.split('\n')
    result = []
    for jel_str in jel_strs:
        prog = re.compile(r'(.*?) - .*')
        match = prog.match(jel_str)
        result.append(match.groups()[0])
    return result


def load_aea(aea_csv):
    return pd.read_csv(aea_csv, encoding_errors='replace')


def aea_earliest_date(row, lowerbound, upperbound):

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

    earliest_ad_text_d = find_best(row['jp_full_text'])
    earliest_official_d = min(
        [parse_(s) for s in [row['Application_deadline']]]
    )
    d = min([earliest_ad_text_d, earliest_official_d])
    return f'{d.year}-{d.month:02d}-{d.day:02d}'


def filter_aea_default(aea):
    aea = aea.copy()
    aea.drop('joe_issue_ID', inplace=True, axis=1)
    aea.drop('jp_agency_insertion_num', inplace=True, axis=1)
    aea['ACADEMIC'] = aea.apply(aea_is_academic, axis=1)
    aea['JEL_Codes'] = aea.apply(
        lambda row: extract_jel_codes(row['JEL_Classifications']),
        axis=1,
    )
    aea['COUNTRIES'] = aea.apply(
        lambda row: countries(row),
        axis=1,
    )
    aea['DISCARD'] = False
    return aea


def aea_applyforthisjoblink(row, bar=None):
    try:
        if not cfg.getlinks:
            return None
        for i in range(cfg.tries):
            try:
                url = row['AD WEBPAGE LINK']
                response = requests.get(url)
                soup = BeautifulSoup(response.content, 'html.parser')
                links = soup.find_all(
                    'a',
                    class_='button',
                    string=lambda text: text and 'Apply for This Job' in text,
                )
                for link in links:
                    if link.get_text() == 'Apply for This Job (link)':
                        typ = 'link'
                        continue
                    if link.get_text() == 'Apply for This Job':
                        typ = 'javascript'
                        continue
                    raise ValueError('No application link found.')
                if typ == 'link':
                    return link['href']
                if typ == 'javascript':
                    return 'JOEWEBAPPLY'
            except Exception as e:
                logger.warning(f'Error for\n{url} on try {i+1}:\n{e}')
            finally:
                time.sleep(random.randint(2, 4))
        return None
    finally:
        if bar is not None:
            bar.update()


def aea_is_academic(row):
    section = row['jp_section'].lower()
    return not ('nonacademic' in section or 'non-academic' in section)


def aea_contains_desired_jel_code(row):
    prog = re.compile('any.field')
    saysanyfield = prog.search(row['jp_full_text']) is not None
    result = row['JEL_Codes'] == [] or any(
        jel_code not in exclude['jel_codes'] for jel_code in row['JEL_Codes']
    )
    return result or saysanyfield


def aea_is_postdoc(row):
    title = row['jp_title'].lower()
    return 'postdoc' in title or 'post-doc' in title or 'post doc' in title


def aea_is_open_rank(row):
    title = row['jp_title'].lower()
    prog = re.compile(r'open.rank')
    return prog.search(title) is not None


def aea_is_full_prof(row):
    title = row['jp_title'].lower()
    prog = re.compile(r'full(?!.time)')
    return prog.search(title) is not None or aea_is_open_rank(row)


def aea_is_associate_prof(row):
    title = row['jp_title'].lower()
    prog = re.compile(
        r'((?<!doc )associate|(?<!doctoral )associate)',
    )
    return prog.search(title) is not None or aea_is_open_rank(row)


def aea_is_assistant_prof(row):
    title = row['jp_title'].lower()
    prog = re.compile(r'(?<!teaching )assistant')
    return prog.search(title) is not None or aea_is_open_rank(row)


def aea_is_lecturer(row):
    title = row['jp_title'].lower()
    prog = re.compile(r'lecture|teach')
    return prog.search(title) is not None


def aea_is_visiting(row):
    title = row['jp_title'].lower()
    return 'visiting' in title


def countries(row):
    prog = re.compile(r'[A-Z\s]*')
    progend = re.compile(r'[A-Z\s]*$')
    result = []
    for location in row['locations'].split('\n'):
        match = prog.match(location)
        matchend = progend.match(location)
        if matchend:
            result.append(matchend.group().strip())
        elif match:
            result.append(match.group()[:-2].strip())
        else:
            print(location)
    return list(set(result))


def aea_webpage_link(row):
    return (
        f'https://www.aeaweb.org/joe/listing.php?JOE_ID=2024-02_{row["jp_id"]}'
    )


def aea_contains_bad_country(row):
    for c in row['COUNTRIES']:
        if c in exclude['countries']:
            return True
    return False


def filter_aea(aea):
    aea = filter_aea_default(aea).copy()
    aea['BAD COUNTRY'] = aea.apply(
        lambda row: (aea_contains_bad_country(row)),
        axis=1,
    )
    aea['BAD JEL CODES'] = aea.apply(
        lambda row: (not aea_contains_desired_jel_code(row)),
        axis=1,
    )
    aea['POSTDOC'] = aea.apply(
        lambda row: (aea_is_postdoc(row)),
        axis=1,
    )
    aea['LECTURER'] = aea.apply(
        lambda row: (aea_is_lecturer(row)),
        axis=1,
    )
    aea['ASSISTANT PROF'] = aea.apply(
        lambda row: (aea_is_assistant_prof(row)),
        axis=1,
    )
    aea['ASSOCIATE PROF'] = aea.apply(
        lambda row: (aea_is_associate_prof(row)),
        axis=1,
    )
    aea['FULL PROF'] = aea.apply(
        lambda row: (aea_is_full_prof(row)),
        axis=1,
    )
    aea['VISITING'] = aea.apply(
        lambda row: (aea_is_visiting(row)),
        axis=1,
    )
    aea['DISCARD'] = aea.apply(
        lambda row: (
            row['BAD COUNTRY'] or (
                row['ACADEMIC'] and (
                    row['BAD JEL CODES'] or row['VISITING'] or row['LECTURER']
                    or (
                        (row['FULL PROF'] or row['ASSOCIATE PROF']) and
                        not (row['ASSISTANT PROF'] or row['POSTDOC'])
                    )
                )
            )
        ),
        axis=1,
    )
    aea['EARLIEST DATE'] = aea.apply(
        aea_earliest_date,
        axis=1,
        lowerbound=parse('10/01/2024'),
        upperbound=parse('12/01/2024'),
    )
    aea['AD WEBPAGE LINK'] = aea.apply(
        aea_webpage_link,
        axis=1,
    )
    bar = tqdm(total=len(aea))
    aea['APPLICATION LINK'] = aea.apply(
        aea_applyforthisjoblink,
        bar=bar,
        axis=1,
    )
    aea['SUBMISSION TYPE'] = aea.apply(
        format_application_link,
        axis=1,
    )
    return aea


def format_application_link(row):
    link = row['APPLICATION LINK']
    if pd.isna(link):
        return None
    if 'JOEWEBAPPLY' in link:
        return 'AEA\'s JOE Submission'
    netloc = urlparse(link).netloc
    if 'interfolio' in netloc:
        return 'Interfolio Submission'
    if 'econjobmarket' in netloc:
        return 'EconJobMarket Submission'
    if 'aea' in netloc:
        return 'AEA\'s JOE Submission'
    return 'Direct email to be sent by Admin'


def format_aea(aea):
    new_aea = pd.DataFrame(
        {
            'Submission Type': aea['SUBMISSION TYPE'],
            'Letter Submission Deadline Date': aea['EARLIEST DATE'],
            'Application Status': pd.NA,
            'Institution or Organization Name': aea['jp_institution'],
            'Department Name': aea['jp_department'],
            'Job ID #': aea['jp_id'],
            'Job Title': aea['jp_title'],
            'Additional Instructions': pd.NA,
            'Ad Webpage Link': aea['AD WEBPAGE LINK'],
            'DISCARD': aea['DISCARD'],
            'ACADEMIC': aea['ACADEMIC'],
        },
    )
    return (
        new_aea[new_aea['DISCARD'] == False]  # noqa
        .drop('DISCARD', axis=1).sort_values(
            by=['Letter Submission Deadline Date', 'Department Name']
        ).reset_index(drop=True)
    )


if __name__ == '__main__':
    raw_aea = load_aea(aea_csv)
    aea = filter_aea(raw_aea)
    verbose = aea.copy()
    verbose = verbose[verbose['DISCARD'] == False]  # noqa
    formatted = format_aea(aea)
    excel = formatted.drop(
        'ACADEMIC',
        axis=1,
    ).reset_index(drop=True)
    discarded = raw_aea.copy()
    discarded['ACADEMIC'] = aea['ACADEMIC']
    discarded = discarded[aea['DISCARD']]
    excel_academic = (
        formatted[formatted['ACADEMIC']].drop('ACADEMIC',
                                              axis=1).reset_index(drop=True)
    )
    excel.to_csv(
        cfg.output / 'aea.csv',
        index=False,
    )
    discarded.to_csv(
        cfg.discarded / 'discarded.csv',
        index=False,
    )
    excel_academic.to_csv(
        cfg.academic / 'aea.csv',
        index=False,
    )
    verbose.to_csv(
        cfg.verbose / 'verbose.csv',
        index=False,
    )
