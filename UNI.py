from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client['study']

advisor_collection = db['advisor']
student_collection = db['student']
student_advisor_collection = db['student_advisor']


def fill_tables():
    advisor_collection.insert_many([
        {"AdvisorID": 1, "AdvisorName": "John Paul"},
        {"AdvisorID": 2, "AdvisorName": "Anthony Roy"},
        {"AdvisorID": 3, "AdvisorName": "Raj Shetty"},
        {"AdvisorID": 4, "AdvisorName": "Sam Reeds"},
        {"AdvisorID": 5, "AdvisorName": "Arthur Clint wood"}
    ])

    student_collection.insert_many([
        {"StudentID": 501, "StudentName": "Geek1"},
        {"StudentID": 502, "StudentName": "Geek2"},
        {"StudentID": 503, "StudentName": "Geek3"},
        {"StudentID": 504, "StudentName": "Geek4"},
        {"StudentID": 505, "StudentName": "Geek5"},
        {"StudentID": 506, "StudentName": "Geek6"},
        {"StudentID": 507, "StudentName": "Geek7"},
        {"StudentID": 508, "StudentName": "Geek8"},
        {"StudentID": 509, "StudentName": "Geek9"},
        {"StudentID": 510, "StudentName": "Geek10"}
    ])

    student_advisor_collection.insert_many([
        {"StudentID": 501, "AdvisorID": 1},
        {"StudentID": 501, "AdvisorID": 3},
        {"StudentID": 502, "AdvisorID": 1},
        {"StudentID": 502, "AdvisorID": 4},
        {"StudentID": 503, "AdvisorID": 3},
        {"StudentID": 504, "AdvisorID": 2},
        {"StudentID": 504, "AdvisorID": 4},
        {"StudentID": 505, "AdvisorID": 4},
        {"StudentID": 506, "AdvisorID": 2},
        {"StudentID": 506, "AdvisorID": 1},
        {"StudentID": 506, "AdvisorID": 3},
        {"StudentID": 507, "AdvisorID": 2},
        {"StudentID": 508, "AdvisorID": 3},
        {"StudentID": 509, "AdvisorID": None},
        {"StudentID": 510, "AdvisorID": 1},
        {"StudentID": 510, "AdvisorID": 5}
    ])


if db["student"].count_documents({}) == 0:
    fill_tables()

pipeline = [
    {"$lookup": {
        "from": "student_advisor",
        "localField": "AdvisorID",
        "foreignField": "AdvisorID",
        "as": "students"
    }},
    {"$project": {
        "_id": 0,  # Exclude the _id field
        "AdvisorID": 1,
        "AdvisorName": 1,
        "number_of_students": {"$size": "$students.StudentID"}
    }}
]

advisor_results = advisor_collection.aggregate(pipeline)

for advisor in advisor_results:
    print(advisor)


client.close()
