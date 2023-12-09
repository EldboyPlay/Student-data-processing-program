# --------------- Подключение к библиотекам --------------- #
import pandas as pd
import json
import sys
import os

# --------------- ОСНОВНОЙ КОД ПРОГРАММЫ --------------- #

# Определение путей к папкам для сохранения новых данных и чтения входных данных
new_data_directory = 'Исходящие данные'
input_data_directory = 'Входящие данные'

# Создание папки для новых данных, если она не существует
if not os.path.exists(new_data_directory):
    os.makedirs(new_data_directory)

# Функция для расчета стипендии на основе среднего балла
def calculate_scholarship(grade):
    if grade >= 4.5:
        return "Высокая"
    elif grade >= 3.5:
        return "Средняя"
    else:
        return "Базовая"

# Функция обработки данных
def process_data(input_dir, output_dir):
    # Загрузка данных о студентах
    students_df = pd.read_csv(os.path.join(input_dir, 'students.csv'), encoding='utf-8-sig')
    # Загрузка данных об оценках
    grades_df = pd.read_csv(os.path.join(input_dir, 'grades.csv'), encoding='utf-8-sig')
    # Загрузка данных о группах
    groups_df = pd.read_csv(os.path.join(input_dir, 'groups.csv'), encoding='utf-8-sig')
    # Загрузка данных о курсах
    courses_df = pd.read_csv(os.path.join(input_dir, 'courses.csv'), encoding='utf-8-sig')

    # Вычисление среднего балла для каждого студента
    avg_grades = grades_df.groupby('stud_id')['grade'].mean().reset_index()
    # Объединение данных о студентах с их средними оценками
    students_with_grades = pd.merge(students_df, avg_grades, on='stud_id', how='left')

    # Отчисление студентов с баллом ниже 3
    expulsion_students = students_with_grades[students_with_grades['grade'] < 3]
    # Сохранение данных об отчисленных студентах
    expulsion_students.to_csv(os.path.join(output_dir, 'expulsion_student.csv'), index=False, encoding='utf-8-sig')

    # Зачисление на следующий курс студентов с баллом 3 и выше
    next_semester_students = students_with_grades[students_with_grades['grade'] >= 3].copy()
    # Расчет стипендии для зачисленных студентов
    next_semester_students.loc[:, 'scholarship'] = next_semester_students['grade'].apply(calculate_scholarship)
    # Сохранение данных о студентах, зачисленных на следующий курс
    next_semester_students.to_csv(os.path.join(output_dir, 'next_semester_student.csv'), index=False, encoding='utf-8-sig')

    # Обновление списка групп на основе зачисленных студентов
    updated_groups = next_semester_students['group_id'].unique()
    next_semester_groups = groups_df[groups_df['group_id'].isin(updated_groups)]
    # Сохранение обновленного списка групп
    next_semester_groups.to_csv(os.path.join(output_dir, 'next_semester_group.csv'), index=False, encoding='utf-8-sig')

    # Выбор студентов с наихудшими результатами
    worst_students = students_with_grades.nsmallest(5, 'grade')['stud_id'].tolist()
    selected_students = {"worst": worst_students}
    # Сохранение информации о студентах с наихудшими результатами
    with open(os.path.join(output_dir, 'selected_student.json'), 'w', encoding='utf-8') as file:
        json.dump(selected_students, file, ensure_ascii=False)

    # Выбор лучшего курса по среднему баллу
    avg_course_grades = grades_df.groupby('course_id')['grade'].mean().reset_index()
    best_course = avg_course_grades.nlargest(1, 'grade')['course_id'].tolist()
    best_course_names = courses_df[courses_df['course_id'].isin(best_course)]['course_name'].tolist()
    selected_courses = {"best": best_course_names}
    # Сохранение информации о лучшем курсе
    with open(os.path.join(output_dir, 'selected_course.json'), 'w', encoding='utf-8') as file:
        json.dump(selected_courses, file, ensure_ascii=False)

    # Выбор группы с наихудшим средним баллом
    avg_group_grades = pd.merge(grades_df, students_df, on='stud_id').groupby('group_id')['grade'].mean().reset_index()
    worst_group = avg_group_grades.nsmallest(1, 'grade')['group_id'].tolist()
    selected_groups = {"worst": worst_group}
    # Сохранение информации о группе с наихудшими результатами
    with open(os.path.join(output_dir, 'selected_group.json'), 'w', encoding='utf-8') as file:
        json.dump(selected_groups, file, ensure_ascii=False)

    # Возврат сообщения об успешной обработке данных
    return "Обработка данных завершена"

# Если скрипт запущен как основная программа
if __name__ == "__main__":
    # Получение директории из аргумента командной строки или использование папки "Входящие данные", если аргумент не предоставлен
    input_directory = sys.argv[1] if len(sys.argv) > 1 else input_data_directory
    # Вызов функции обработки данных с указанием входной и выходной директорий
    result = process_data(input_directory, new_data_directory)
    print(result)
