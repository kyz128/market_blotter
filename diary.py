from flask import Flask
from flask_restx import Api, Resource, Namespace, fields, inputs
import datetime
from flask_pymongo import PyMongo
import pymongo
from werkzeug.routing import BaseConverter, ValidationError

app = Flask(__name__)
app.config["MONGO_URI"] = "mongodb+srv://<username>:<password>@cluster0.po32h.mongodb.net/<collection_name>?retryWrites=true&w=majority"
api = Api(app, title = "Trader Diary API")
ns = api.namespace('entries', description='Trader diary CRUD operations')
mongo = PyMongo(app)


#################################################################################################################################
#Business Logic
#################################################################################################################################

class DiaryEntry(object):
    def __init__(self):
        self.db = mongo.db.diaries
    def show(self):
        all_entries = list(self.db.find({}, {"_id": 0}).sort("entry_date", pymongo.DESCENDING))
        return all_entries

    def filter_by_dates(self, dates):
        s_date = dates["start_date"]
        start = datetime.datetime(s_date.year, s_date.month, s_date.day)
        e_date = datetime.date.today() if dates["end_date"] == None else dates["end_date"]
        end = datetime.datetime(e_date.year, e_date.month, e_date.day)
        res = list(self.db.find({"entry_date": {'$lte': end, '$gte': start}}, {"_id": 0}).sort("entry_date",pymongo.DESCENDING))
        return res

    def filter_by_tag(self, tag):
        res = list(self.db.find({"tags": tag}, {"_id": 0}).sort("entry_date",pymongo.DESCENDING))
        return res

    def mistake_stats(self):
        pipeline = [{"$unwind": "$tags"},
                    {"$group": {"_id": "$tags",
                                "count": { "$sum": 1 }
                                }
                    }]
        res = list(self.db.aggregate(pipeline))
        return res

    def details(self, date):
        start = datetime.datetime(date.year, date.month, date.day)
        end = start + datetime.timedelta(days=1)  
        res = self.db.find_one({"entry_date": {'$lt': end, '$gte': start}}, {"_id":0})
        if res:
            return res
        api.abort(404, "Entry from {} doesn't exist".format(date))

    def create(self, data):
        date = data["entry_date"]
        data["entry_date"] = datetime.datetime(date.year, date.month, date.day)
        self.db.insert_one(data)
        return data

    def update(self, date, data):
        start = datetime.datetime(date.year, date.month, date.day)        
        end = start + datetime.timedelta(days=1)
        query = {"entry_date": {'$lt': end, '$gte': start}}
        orig_data = self.details(date)
        for k in data.keys():
            if data[k]:
                orig_data[k] = data[k]
        self.db.update_one(query, {"$set": orig_data})
        return self.details(date)

    def delete(self, date):
        start = datetime.datetime(date.year, date.month, date.day)
        end = start + datetime.timedelta(days=1)
        query = {"entry_date": {'$lt': end, '$gte': start}}
        self.db.delete_one(query)
        return

#################################################################################################################################
#URL Validation 
#################################################################################################################################

class DateConverter(BaseConverter):
    """Extracts a ISO8601 date from the path and validates it."""

    regex = r'\d{4}-\d{2}-\d{2}'

    def to_python(self, value):
        try:
            return datetime.datetime.strptime(value, '%Y-%m-%d').date()
        except ValueError:
            raise ValidationError()
 
    def to_url(self, value):
        return value.strftime('%Y-%m-%d')

app.url_map.converters['date'] = DateConverter
DE = DiaryEntry()

#################################################################################################################################
#Display model; part of API documentation 
#################################################################################################################################

entry = ns.model('diary entry', {
    'entry_date': fields.DateTime(required=True, description="Date entry created"),
    'entry': fields.String(required=True, description="Entry text"),
    'trade': fields.String(description="Trades placed on entry date if any"),
    'weekly_reflection': fields.String(description="End of week reflection of what happened"),
    'tags': fields.List(fields.String, description = "Keywords summarizing weekly mistakes if any")
})

#################################################################################################################################
#Parsers
#################################################################################################################################

parser = api.parser()
update_parser = parser.copy()
update_parser.add_argument('entry', type=str, help = "Daily observation to be included diary entry")
update_parser.add_argument('trade', type = str, help= "Trades placed on entry date if any")
update_parser.add_argument('weekly_reflection', type = str, help = "End of week reflection of what happened")
update_parser.add_argument('tags', type = str, action = "split", help= "Weekly mistake categorization")
create_parser = update_parser.copy()
create_parser.add_argument('entry_date', type=inputs.date, required = True, help='Date entry is/will be entered (YYYY-MM-DD)')
create_parser.replace_argument('entry', type=str, required= True, help = "Daily observation to be included diary entry")
filter_parser = parser.copy()
filter_parser.add_argument('start_date', type= inputs.date, required = True, help = "Filter date range (lower)")
filter_parser.add_argument('end_date', type= inputs.date, help = "Filter date range (upper); will default to today if not provided")
tag_parser = parser.copy()
tag_parser.add_argument('tag', type = str, required = True, help = "Filter by mistake made")


#################################################################################################################################
#Routes
#################################################################################################################################

@ns.route('/')
class EntryList(Resource):
    '''Shows a list of all entries, and lets you POST to add new tasks'''
    @ns.doc('list_entries')
    @ns.marshal_list_with(entry)
    def get(self):
        '''List all entries'''
        return DE.show()

    @ns.doc('create_entry')
    @ns.expect(create_parser)
    @ns.marshal_with(entry)
    def post(self):
        '''Create a new entry'''
        data = create_parser.parse_args()
        return DE.create(data)


@ns.route('/<date:entry_date>')
@ns.response(404, 'Entry not found')
class Entry(Resource):
    '''Show a single entry'''
    @ns.doc('get_entry')
    @ns.marshal_with(entry)
    def get(self, entry_date):
        '''Fetch an entry given its date created'''
        return DE.details(entry_date)

    @ns.doc('delete_entry')
    @ns.response(204, 'Entry deleted')
    def delete(self, entry_date):
        '''Delete an entry given its date created'''
        DE.delete(entry_date)
        return '', 204

    @ns.expect(update_parser)
    @ns.marshal_with(entry)
    def put(self, entry_date):
        '''Update an entry given its date created'''
        data = update_parser.parse_args()
        return DE.update(entry_date, data)

@ns.route('/date_filter')
class DateRangeEntry(Resource):
    @ns.doc("filter_entries_by_dates")
    @ns.expect(filter_parser)
    @ns.marshal_list_with(entry)
    def get(self):
        '''Filter entries by date ranges'''
        data = filter_parser.parse_args()
        return DE.filter_by_dates(data)

@ns.route('/tag_filter')
class TagEntry(Resource):
    @ns.doc("filter_entries_by_tag")
    @ns.expect(tag_parser)
    @ns.marshal_list_with(entry)
    def get(self):
        '''Filter entries by tag'''
        data = tag_parser.parse_args()
        return DE.filter_by_tag(data["tag"])

@ns.route('/mistake_stats')
class MistakeCount(Resource):
    @ns.doc("count_entries_by_tag")
    def get(self):
        '''Show tag counts'''
        return DE.mistake_stats()



if __name__ == '__main__':
    app.run(debug=True)
