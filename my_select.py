from sqlalchemy import func, desc, select, and_
from conf.models import Grade, Teacher, Student, Group, Subject
from conf.db import session


def select_1():
    return session.query(
        Student.fullname,
        func.avg(Grade.grade).label('average_grade')
    ).join(Grade, Student.id == Grade.student_id) \
        .group_by(Student.id) \
        .order_by(func.avg(Grade.grade).desc()) \
        .limit(5) \
        .all()


def select_2(subject_id):
    return session.query(
        Student.fullname,
        func.avg(Grade.grade).label('average_grade')
    ).join(Grade, Student.id == Grade.student_id) \
        .filter(Grade.subjects_id == subject_id) \
        .group_by(Student.id) \
        .order_by(func.avg(Grade.grade).desc()) \
        .first()


def select_3(subject_id):
    return session.query(
        Group.name,
        func.avg(Grade.grade).label('average_grade')
    ).join(Student, Group.id == Student.group_id)\
     .join(Grade, Student.id == Grade.student_id)\
     .filter(Grade.subjects_id == subject_id)\
     .group_by(Group.id)\
     .all()


def select_4():
    return session.query(
        func.avg(Grade.grade).label('average_grade')
    ).scalar()


def select_5(teacher_id):
    return session.query(
        Subject.name
    ).filter(Subject.teacher_id == teacher_id)\
     .all()


def select_6(group_id):
    return session.query(
        Student.fullname
    ).filter(Student.group_id == group_id)\
     .all()


def select_7(group_id, subject_id):
    return session.query(
        Student.fullname,
        Grade.grade
    ).join(Grade, Student.id == Grade.student_id)\
     .filter(Student.group_id == group_id, Grade.subjects_id == subject_id)\
     .all()


def select_8(teacher_id):
    return session.query(
        func.avg(Grade.grade).label('average_grade')
    ).join(Subject, Grade.subjects_id == Subject.id)\
     .filter(Subject.teacher_id == teacher_id)\
     .scalar()


def select_9(student_id):
    return session.query(
        Subject.name
    ).join(Grade, Subject.id == Grade.subjects_id)\
     .filter(Grade.student_id == student_id)\
     .group_by(Subject.id)\
     .all()


def select_10(student_id, teacher_id):
    return session.query(
        Subject.name
    ).join(Grade, Subject.id == Grade.subjects_id)\
     .filter(Grade.student_id == student_id, Subject.teacher_id == teacher_id)\
     .group_by(Subject.id)\
     .all()


def select_11(student_id, teacher_id):
    return session.query(
        func.avg(Grade.grade).label('average_grade')
    ).join(Student, Grade.student_id == Student.id)\
     .join(Subject, Grade.subjects_id == Subject.id)\
     .filter(Student.id == student_id, Subject.teacher_id == teacher_id)\
     .scalar()


def select_12(group_id, subject_id):
    subquery = session.query(
        func.max(Grade.grade_date)
    ).join(Student, Grade.student_id == Student.id)\
     .filter(Student.group_id == group_id, Grade.subjects_id == subject_id)\
     .scalar()

    return session.query(
        Student.fullname,
        Grade.grade
    ).join(Grade, Student.id == Grade.student_id)\
     .filter(Student.group_id == group_id, Grade.subjects_id == subject_id, Grade.grade_date == subquery)\
     .all()


session.close()

if __name__ == '__main__':
    print(select_1())
    print(select_2())
    print(select_3())
    print(select_4())
    print(select_5())
    print(select_6())
    print(select_7())
    print(select_8())
    print(select_9())
    print(select_10())
