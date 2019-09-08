# coding: utf-8
import re
import click
import pandas as pd
import simplejson

@click.command()
class Main(object):
    def __init__(self):
        source = 'Zauberflöte Timestamps.xlsx'
        perf_df = pd.read_excel(source, 'Performances')
        cat_df = pd.read_excel(source, 'Catalogue Extract', converters={'dat': str})
        cat_df['art'] = cat_df['art'].map(self.fix_name)
        tapeseg_df = pd.read_excel(source, 'Tapes-Segmentation')
        workseg_df = pd.read_excel(source, 'Zauberflöte-Segments')
        seg_df = workseg_df.join(tapeseg_df, lsuffix='_caller', rsuffix='_other')
        seg_df.rename(columns={'Segment-ID_caller': 'ID', 'Segment-Label_caller': 'Label'}, inplace=True)
        seg_df.drop(columns=['Segment-Label_other'], inplace=True)
        roles_df = pd.read_excel(source, 'CharacterRoles')
        pers_df = pd.read_excel(source, 'Persons')
        pers_df['Label'] = pers_df['Label'].map(self.fix_name)
        
        # persons
        pers_dict = dict()
        for _, pers_row in pers_df.iterrows():
            label = pers_row['Label']
            if not pd.isnull(pers_row['Image']):
                img_url = pers_row['Image']
            else:
                img_url = None
            if not pd.isnull(pers_row['Wikidata-Q']):
                wikidata_uri = 'https://entity.wikidata.org/{}'.format(pers_row['Wikidata-Q'])
            else:
                wikidata_uri = None
            pers_dict[label] = Artist(label, img_url, wikidata_uri)

        # roles
        roles_dict = dict()
        for _, roles_row in roles_df.iterrows():
            roles_dict[roles_row['CharacterRole-ID']] = []
            for r in re.split('\s*;\s*', roles_row['rol']):
                label = r
                wikidata_uri = None
                group = None
                if not pd.isnull(roles_row['Label']):
                    label = roles_row['Label']
                if not pd.isnull(pers_row['Wikidata-Q']):
                    wikidata_uri = 'https://entity.wikidata.org/{}'.format(pers_row['Wikidata-Q'])
                if r != roles_row['CharacterRole-ID']:
                    group = roles_row['CharacterRole-ID']
                role = Role(label, wikidata_uri, group)
                roles_dict[roles_row['CharacterRole-ID']].append(role)
                if label not in roles_dict:
                    roles_dict[label] = [role]

        work = Work('Die Zauberflöte', 'Mozart')
        
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

        with open('export.json', 'w') as jsonfile:
            jsonfile.write(simplejson.dumps(data, encoding='utf-8', indent=4, sort_keys=False))

    def parse_seg(self, row, perf_dict, roles_dict):
        id = int(row['ID'])
        seg_type = row['Segment-Type']
        recordings = [k[:-6] for k in row.keys().values if k[-6:] == '-Begin']
        for r in recordings:
            if f'{r}-Begin' in row and not pd.isnull(row[f'{r}-Begin']) and f'{r}-End' in row and not pd.isnull(row[f'{r}-End']):
                seg = Segment(id, seg_type, perf_dict[r].id, r, str(row[f'{r}-Begin']), str(row[f'{r}-End']))
                if not pd.isnull(row['CharacterRoles']):
                    seg.set_roles(row['CharacterRoles'], roles_dict)
                    for role in re.split(';\s*', row['CharacterRoles']):
                        if role in perf_dict[r].roles_dict:
                            seg.add_artist(perf_dict[r].roles_dict[role], role)
                if r in perf_dict:
                    perf_dict[r].add_segment(seg)

    def fix_name(self, name: str):
        s = re.split('\s*,\s*', name)
        s.reverse()
        return ' '.join(s).title()


class Role(object):
    def __init__(self, label:str, wikidata_uri:str=None, group:str=None):
        self.label = label
        self.wikidata_uri = wikidata_uri
        self.group = group

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
    def __init__(self, id:int, type_:str, perf_id:str, recording:str = None, start:str = None, end:str = None):
        self.id = id
        self.type = type_
        self.roles = []
        self.artists = []
        if recording:
            track = re.search('(?<=Track)[0-9]+', recording).group(0)
            channel = re.search('(?<=Channel)[0-9]+', recording).group(0)
            self.audio_url = f'https://operatinder.s3.amazonaws.com/{perf_id}-T{str(track).zfill(3)}-C{channel}_Q5064_{str(self.id).zfill(2)}.mp3'
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
        self.cast = []
        self.segments = []
        self.cast = dict()
        self.roles_dict = dict()

    def get_recording(self):
        return self.recording

    def parse_cat(self, cat_row:pd.Series, roles_dict:dict):
        self.venue = cat_row['venue']
        self.date = cat_row['dat'][:10]
        self.id = cat_row['ide']
        if not pd.isnull(cat_row['rol']) and not pd.isnull(cat_row['art']):
            self.cast[cat_row['rol']] = cat_row['art']

    def add_segment(self, segment:Segment):
        self.segments.append(segment)

    def add_cast(self, role:Role):
        self.cast.append(role)

    def to_object(self):
        data = {}
        if self.venue:
            data['venue'] = self.venue
        if self.date:
            data['date'] = self.date
        if self.cast:
            data['cast'] = []
            for r, a in self.cast.items():
                data['cast'].append({r: a})
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
        self.performances = []

    def add_performance(self, performance:Performance):
        self.performances.append(performance)

    def to_object(self):
        data = {}
        data['title'] = self.title
        if self.composer:
            data['composer'] = self.composer
        if self.performances:            
            data['performances'] = []
            for p in self.performances:
                data['performances'].append(p.to_object())
        return data


if __name__ == '__main__':
    m = Main()