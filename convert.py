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
        tapeseg_df = pd.read_excel(source, 'Tapes-Segmentation')
        workseg_df = pd.read_excel(source, 'Zauberflöte-Segments')
        seg_df = workseg_df.join(tapeseg_df, lsuffix='_caller', rsuffix='_other')
        seg_df.rename(columns={'Segment-ID_caller': 'ID', 'Segment-Label_caller': 'Label'}, inplace=True)
        seg_df.drop(columns=['Segment-Label_other'], inplace=True)
        roles_df = pd.read_excel(source, 'CharacterRoles')
        pers_df = pd.read_excel(source, 'Persons')
        
        # persons
        pers_dict = dict()
        for _, pers_row in pers_df.iterrows():
            pers_dict[pers_row['Label']] = dict()
            if not pd.isnull(pers_row['Wikidata-Q']):
                pers_dict[pers_row['Label']]['wikidata'] = 'https://entity.wikidata.org/{}'.format(pers_row['Wikidata-Q'])
            if not pd.isnull(pers_row['Image']):
                pers_dict[pers_row['Label']]['image_url'] = pers_row['Image']

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
                perf.parse_cat(cat_row)
            else:
                print(cat_row['WAV-Recording'], ' not found!')

        # segments
        for _, row in seg_df.iterrows():
            _ = self.parse_seg(row, perf_dict)

        data = {}
        data['works'] = [work.to_object()]

        with open('export.json', 'w') as jsonfile:
            jsonfile.write(simplejson.dumps(data, encoding='utf-8', indent=4, sort_keys=False))

    def parse_seg(self, row, perf_dict):
        id = int(row['ID'])
        seg_type = row['Segment-Type']
        recordings = [k[:-6] for k in row.keys().values if k[-6:] == '-Begin']
        for r in recordings:
            if f'{r}-Begin' in row and not pd.isnull(row[f'{r}-Begin']) and f'{r}-End' in row and not pd.isnull(row[f'{r}-End']):
                seg = Segment(id, seg_type, perf_dict[r].id, r, str(row[f'{r}-Begin']), str(row[f'{r}-End']))
                if not pd.isnull(row['CharacterRoles']):
                    seg.set_roles(row['CharacterRoles'])
                    for role in re.split(';\s*', row['CharacterRoles']):
                        if role in perf_dict[r].roles:
                            seg.add_artist(perf_dict[r].roles[role], role)
                if r in perf_dict:
                    perf_dict[r].add_segment(seg)

class Segment(object):
    def __init__(self, id:int, type_:str, perf_id:str, recording:str = None, start:str = None, end:str = None):
        self.id = id
        self.type = type_
        self.roles = None
        self.artists = []
        if recording:
            track = re.search('(?<=Track)[0-9]+', recording).group(0)
            channel = re.search('(?<=Channel)[0-9]+', recording).group(0)
            self.audio_url = f'https://operatinder.s3.amazonaws.com/{perf_id}-T{track}-C{channel}_Q5064_{self.id}.mp3'
            self.start = start
            self.end = end
    
    def set_roles(self, roles):
        self.roles = roles.replace(';', ',')

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
        self.segments = []
        self.roles = dict()

    def get_recording(self):
        return self.recording

    def parse_cat(self, cat_row):
        self.venue = cat_row['venue']
        self.date = cat_row['dat'][:10]
        self.id = cat_row['ide']
        if not pd.isnull(cat_row['rol']) and not pd.isnull(cat_row['art']):
            artist = cat_row['art'].split(', ')
            artist.reverse()
            self.roles[cat_row['rol']] = ' '.join(artist).title()

    def add_segment(self, segment:Segment):
        self.segments.append(segment)

    def to_object(self):
        data = {}
        if self.venue:
            data['venue'] = self.venue
        if self.date:
            data['date'] = self.date
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