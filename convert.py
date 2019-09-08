# coding: utf-8
import re
import click
import pandas as pd
import simplejson
import gspread
from oauth2client.service_account import ServiceAccountCredentials

@click.command()
@click.option('--source', default='Zauberflöte Timestamps.xlsx', help='local source spreadsheet, use "GoogleDoc" to use the online version')
@click.option('--target', default='data.json', help='target JSON file')
class Main(object):
    def __init__(self, source:str, target:str):
        if source.lower() == 'googledoc':
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            credentials = ServiceAccountCredentials.from_json_keyfile_name('OperaForeverData-7c45f8590f8c.json', scope)
            gc = gspread.authorize(credentials)
            spreadsheet = gc.open_by_url('https://docs.google.com/spreadsheets/d/1kNxlphekMp2RfHPrZ0hg_f3gTCczspbaU9poTDQ-FgU/edit#gid=1976298026')
            perf_df = self.get_google_sheet(spreadsheet, 0)
            cat_df = self.get_google_sheet(spreadsheet, 1)
            tapeseg_df = self.get_google_sheet(spreadsheet, 2)
            workseg_df = self.get_google_sheet(spreadsheet, 3)
            roles_df = self.get_google_sheet(spreadsheet, 4)
            pers_df = self.get_google_sheet(spreadsheet, 5)
        else:
            perf_df = pd.read_excel(source, 'Performances')
            cat_df = pd.read_excel(source, 'Catalogue Extract', converters={'dat': str})
            tapeseg_df = pd.read_excel(source, 'Tapes-Segmentation')
            workseg_df = pd.read_excel(source, 'Zauberflöte-Segments')
            roles_df = pd.read_excel(source, 'CharacterRoles')
            pers_df = pd.read_excel(source, 'Persons')

        # cleaning up
        cat_df['art'] = cat_df['art'].map(self.fix_name)
        seg_df = workseg_df.join(tapeseg_df, lsuffix='_caller', rsuffix='_other')
        seg_df.rename(columns={'Segment-ID_caller': 'ID', 'Segment-Label_caller': 'Label'}, inplace=True)
        seg_df.drop(columns=['Segment-Label_other'], inplace=True)
        pers_df['Label'] = pers_df['Label'].map(self.fix_name)
        
        work = Work('Die Zauberflöte', 'Mozart')

        # persons
        pers_dict = dict()
        for _, pers_row in pers_df.iterrows():
            label = pers_row['Label']
            img_url, wikidata_uri = None, None
            if not pd.isnull(pers_row['Image']):
                img_url = pers_row['Image']
            if not pd.isnull(pers_row['Wikidata-Q']) and str(pers_row['Wikidata-Q']) != '':
                wikidata_uri = 'http://entity.wikidata.org/{}'.format(pers_row['Wikidata-Q'])
            pers_dict[label] = Artist(label, img_url, wikidata_uri)

        # roles
        roles_dict = dict()
        for _, roles_row in roles_df.iterrows():
            roles_dict[roles_row['CharacterRole-ID']] = []
            for r in re.split('\s*;\s*', roles_row['rol']):
                label = r
                wikidata_uri, group = None, None
                if not pd.isnull(roles_row['Label']) and str(roles_row['Label']) != '':
                    label = roles_row['Label']
                if not pd.isnull(pers_row['Wikidata-Q']):
                    wikidata_uri = 'http://entity.wikidata.org/{}'.format(pers_row['Wikidata-Q'])
                if r != roles_row['CharacterRole-ID']:
                    group = roles_row['CharacterRole-ID']
                role = Role(label, wikidata_uri, group)
                roles_dict[roles_row['CharacterRole-ID']].append(role)
                if label not in roles_dict:
                    roles_dict[label] = [role]
                work.add_role(role)

        # performances
        perf_dict = dict()
        for _, perf_row in perf_df.iterrows():
            perf = Performance(perf_row['WAV-Recording'])
            perf_dict[perf.get_recording()] = perf
            work.add_performance(perf)

        for _, cat_row in cat_df.iterrows():
            if cat_row['WAV-Recording'] in perf_dict:
                perf = perf_dict[cat_row['WAV-Recording']]
                perf.parse_cat(cat_row, roles_dict)
            else:
                print(cat_row['WAV-Recording'], ' not found!')

        # segments
        for _, row in seg_df.iterrows():
            _ = self.parse_seg(row, perf_dict, roles_dict)

        data = {}
        data['artists'] = []
        for _, a in pers_dict.items():
            data['artists'].append(a.to_object())
        data['works'] = [work.to_object()]

        with open(target, 'w') as jsonfile:
            jsonfile.write(simplejson.dumps(data, encoding='utf-8', indent=4, sort_keys=False))

    def parse_seg(self, row, perf_dict, roles_dict):
        id = int(row['ID'])
        label = row['Label']
        seg_type = row['Segment-Type']
        recordings = [k[:-6] for k in row.keys().values if k[-6:] == '-Begin']
        for r in recordings:
            start_tc, end_tc = self.get_start_end(row[f'{r}-Begin'], row[f'{r}-End'])
            if start_tc and end_tc:
                seg = Segment(id, label, seg_type, perf_dict[r].id, r, start_tc, end_tc)
                if not pd.isnull(row['CharacterRoles']):
                    seg.set_roles(row['CharacterRoles'], roles_dict)
                    for role in re.split(';\s*', row['CharacterRoles']):
                        if role in perf_dict[r].roles_dict:
                            seg.add_artist(perf_dict[r].roles_dict[role], role)
                if r in perf_dict:
                    perf_dict[r].add_segment(seg)

    def get_google_sheet(self, spreadsheet:gspread.Spreadsheet, sheet:int):
        worksheet = spreadsheet.get_worksheet(sheet)
        data = worksheet.get_all_values()
        headers = data.pop(0)
        df = pd.DataFrame(data, columns=headers)
        return df

    def fix_name(self, name: str):
        s = re.split('\s*,\s*', name)
        s.reverse()
        return ' '.join(s).title()

    def get_start_end(self, start_field, end_field):
        start, end = None, None
        start_search = re.search('\d{2}:\d{2}:\d{2}', str(start_field))
        if start_search:
            start = start_search.group(0)
        end_search = re.search('\d{2}:\d{2}:\d{2}', str(end_field))
        if end_search:
            end = end_search.group(0)
        return start, end


class Role(object):
    def __init__(self, label:str, wikidata_uri:str=None, group:str=None):
        self.label = label
        self.wikidata_uri = wikidata_uri
        self.group = group

    def to_object(self):
        data = {}
        data['label'] = self.label
        if self.wikidata_uri:
            data['wikidata_uri'] = self.wikidata_uri
        if self.group:
            data['group'] = self.group
        return data


class Artist(object):
    def __init__(self, label:str, img_url:str=None, wikidata_uri:str=None):
        self.label = label
        self.img_url = img_url
        self.wikidata_uri = wikidata_uri

    def to_object(self):
        data = {}
        data['label'] = self.label
        if self.img_url:
            data['img_url'] = self.img_url
        if self.wikidata_uri:
            data['wikidata_uri'] = self.wikidata_uri
        return data

class Segment(object):
    def __init__(self, id:int, label:str, type_:str, perf_id:str, recording:str = None, start:str = None, end:str = None):
        self.id = id
        self.label = label
        self.type = type_
        self.roles = []
        self.artists = []
        if recording:
            track = re.search('(?<=Track)[0-9]+', recording).group(0)
            channel = re.search('(?<=Channel)[0-9]+', recording).group(0)
            self.audio_url = f'https://operatinder.s3.amazonaws.com/{perf_id}-T{track}-C{channel}_Q5064_{str(self.id).zfill(2)}.mp3'
            self.start = start
            self.end = end
    
    def set_roles(self, roles, roles_dict):
        for r in re.split('\s*;\s*', roles):
            if r in roles_dict:
                for rr in roles_dict[r]:
                    self.roles.append(rr.label)

    def add_artist(self, artist:str, role:str):
        self.artists.append(f'{artist} ({role})')

    def to_object(self):
        data = {}
        data['id'] = self.id
        data['label'] = self.label
        data['type'] = self.type
        if self.roles:
            data['roles'] = self.roles
        if self.artists:
            data['artists'] = ', '.join(self.artists)
        data['audio_url'] = self.audio_url
        if self.start:
            data['start'] = self.start
        if self.end:
            data['end'] = self.end
        return data

class Performance(object):
    def __init__(self, recording:str):
        self.recording = recording
        self.id = None
        self.venue = None
        self.date = None
        self.cast = dict()
        self.segments = []
        self.roles_dict = dict()

    def get_recording(self):
        return self.recording

    def parse_cat(self, cat_row:pd.Series, roles_dict:dict):
        self.venue = cat_row['venue']
        self.date = cat_row['dat'][:10]
        self.id = '{}{}'.format(cat_row['ide'][:4], cat_row['ide'][4:].zfill(3))
        if not pd.isnull(cat_row['rol']) and str(cat_row['rol']) != '' and not pd.isnull(cat_row['art']) and str(cat_row['art']) != '':
            self.cast[cat_row['rol']] = cat_row['art']

    def add_segment(self, segment:Segment):
        self.segments.append(segment)

    def to_object(self):
        data = {}
        if self.venue:
            data['venue'] = self.venue
        if self.date:
            data['date'] = self.date
        if self.cast:
            data['cast'] = []
            for r, a in self.cast.items():
                data['cast'].append({'role': r, 'artist': a})
        data['id'] = self.id
        data['recording'] = self.recording
        if self.segments:
            data['segments'] = []
            for s in self.segments:
                data['segments'].append(s.to_object())
        return data

class Work(object):
    def __init__(self, title_:str, composer:str=None):
        self.title = title_
        self.composer = composer
        self.roles = []
        self.performances = []

    def add_performance(self, performance:Performance):
        self.performances.append(performance)

    def add_role(self, role:Role):
        self.roles.append(role)

    def to_object(self):
        data = {}
        data['title'] = self.title
        if self.composer:
            data['composer'] = self.composer
        if self.roles:            
            data['roles'] = []
            for r in self.roles:
                data['roles'].append(r.to_object())
        if self.performances:            
            data['performances'] = []
            for p in self.performances:
                data['performances'].append(p.to_object())
        return data


if __name__ == '__main__':
    m = Main()