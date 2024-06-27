PASSING_GRADE = 8


class Trainee:
    visited_lectures = 0
    done_home_tasks = 0
    missed_lectures = 0
    missed_home_tasks = 0
    mark = 0

    def __init__(self, name: str, surname: str):
        self.name = name
        self.surname = surname

    def visit_lecture(self):
        self.visited_lectures += 1
        # make sure to have more 0 and less or equal 10
        if 0 <= self.mark < 10:
            self._add_points(1)
        return self.visited_lectures

    def do_homework(self):
        self.done_home_tasks += 2
        self._add_points(2)  # call _add_points method
        return self.done_home_tasks

    def miss_lecture(self):
        self.missed_lectures -= 1
        self._subtract_points(1)
        return self.missed_lectures

    def miss_homework(self):
        self.missed_home_tasks -= 2
        self._subtract_points(2)
        return self.missed_home_tasks

    def _add_points(self, points: int):
        if self.mark < 10:
            if self.mark + points <= 10:
                self.mark += points
            # if we are trying to add 2 when mark is 9
            # we need to calculate differance
            else:
                self.mark += 10 - self.mark

    def _subtract_points(self, points):
        if 0 < self.mark <= 10:
            if self.mark - points >= 0:
                self.mark -= points
            # if we are trying to subtract 2 when mark is 1
            # we need to make mark 0
            else:
                self.mark -= self.mark
        return self.mark

    def is_passed(self):
        if self.mark >= 8:
            print("Good job!")
        else:
            print(f"You need to {8 - self.mark} points. Try to do your best!")

    def __str__(self):
        status = (
            f"Trainee {self.name.title()} {self.surname.title()}:\n"
            f"done homework {self.done_home_tasks} points;\n"
            f"missed homework {self.missed_home_tasks} points;\n"
            f"visited lectures {self.visited_lectures} points;\n"
            f"missed lectures {self.missed_lectures} points;\n"
            f"current mark {self.mark};\n"
        )
        return status
