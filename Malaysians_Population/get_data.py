import pandas as pd

def get_birth_rate():
    URL_DATA = 'https://storage.dosm.gov.my/demography/birth.parquet'
    df = pd.read_parquet(URL_DATA)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year
    return df

def get_birth_state():
    URL_DATA = 'https://storage.dosm.gov.my/demography/birth_state.parquet'
    df = pd.read_parquet(URL_DATA)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year
    return df

def get_stillbirth():
    URL_DATA = 'https://storage.dosm.gov.my/demography/stillbirth.parquet'
    df = pd.read_parquet(URL_DATA)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year
    return df

def get_birth_district_sex():
    URL_DATA = 'https://storage.dosm.gov.my/demography/birth_district_sex.parquet'
    df = pd.read_parquet(URL_DATA)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year
    return df

def get_stillbirth_state():
    URL_DATA = 'https://storage.dosm.gov.my/demography/stillbirth_state.parquet'
    df = pd.read_parquet(URL_DATA)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year
    return df

def get_birth_sex_ethnic():
    URL_DATA = 'https://storage.dosm.gov.my/demography/birth_sex_ethnic.parquet'
    df = pd.read_parquet(URL_DATA)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year
    return df

def get_birth_sex_ethnic_state():
    URL_DATA = 'https://storage.dosm.gov.my/demography/birth_sex_ethnic_state.parquet'
    df = pd.read_parquet(URL_DATA)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year
    return df

def get_population_malaysia():
    URL_DATA = 'https://storage.dosm.gov.my/population/population_malaysia.parquet'
    df = pd.read_parquet(URL_DATA)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year
    return df

def get_population_state():
    URL_DATA = 'https://storage.dosm.gov.my/population/population_state.parquet'
    df = pd.read_parquet(URL_DATA)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year
    return df

def get_population_district():
    URL_DATA = 'https://storage.dosm.gov.my/population/population_district.parquet'
    df = pd.read_parquet(URL_DATA)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year
    return df

def get_population_parlimen():
    URL_DATA = 'https://storage.dosm.gov.my/population/population_parlimen.parquet'
    df = pd.read_parquet(URL_DATA)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year
    return df

def get_population_dun():
    URL_DATA = 'https://storage.dosm.gov.my/population/population_dun.parquet'
    df = pd.read_parquet(URL_DATA)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year
    return df

def get_death():
    URL_DATA = 'https://storage.dosm.gov.my/demography/death.parquet'
    df = pd.read_parquet(URL_DATA)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year
    return df

def get_death_state():
    URL_DATA = 'https://storage.dosm.gov.my/demography/death_state.parquet'
    df = pd.read_parquet(URL_DATA)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year
    return df

def get_death_sex_ethnic():
    URL_DATA = 'https://storage.dosm.gov.my/demography/death_sex_ethnic.parquet'
    df = pd.read_parquet(URL_DATA)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year
    return df

def get_death_sex_ethnic_state():
    URL_DATA = 'https://storage.dosm.gov.my/demography/death_sex_ethnic_state.parquet'
    df = pd.read_parquet(URL_DATA)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year
    return df

def get_death_district_sex():
    URL_DATA = 'https://storage.dosm.gov.my/demography/death_district_sex.parquet'
    df = pd.read_parquet(URL_DATA)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year
    return df

def get_death_perinatal():
    URL_DATA = 'https://storage.dosm.gov.my/demography/death_perinatal.parquet'
    df = pd.read_parquet(URL_DATA)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year
    return df

def get_death_neonatal_state():
    URL_DATA = 'https://storage.dosm.gov.my/demography/death_neonatal_state.parquet'
    df = pd.read_parquet(URL_DATA)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year
    return df

def get_death_infant():
    URL_DATA = 'https://storage.dosm.gov.my/demography/death_infant.parquet'
    df = pd.read_parquet(URL_DATA)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year
    return df

def get_death_infant_state():
    URL_DATA = 'https://storage.dosm.gov.my/demography/death_infant_state.parquet'
    df = pd.read_parquet(URL_DATA)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year
    return df

def get_death_toddler():
    URL_DATA = 'https://storage.dosm.gov.my/demography/death_toddler.parquet'
    df = pd.read_parquet(URL_DATA)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year
    return df

def get_death_toddler_state():
    URL_DATA = 'https://storage.dosm.gov.my/demography/death_toddler_state.parquet'
    df = pd.read_parquet(URL_DATA)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year
    return df

def get_death_maternal():
    URL_DATA = 'https://storage.dosm.gov.my/demography/death_maternal.parquet'
    df = pd.read_parquet(URL_DATA)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year
    return df

def get_death_maternal_state():
    URL_DATA = 'https://storage.dosm.gov.my/demography/death_maternal_state.parquet'
    df = pd.read_parquet(URL_DATA)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year
    return df

birth_rate = get_birth_rate()
birth_state = get_birth_state()
birth_state_sex = get_birth_sex_ethnic_state()
stillbirth = get_stillbirth()
birth_district_sex = get_birth_district_sex()
stillbirth_state = get_stillbirth_state()
birth_sex_ethnic = get_birth_sex_ethnic_state()
population_malaysia = get_population_malaysia()
population_state = get_population_state()

