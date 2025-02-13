import dill
import random as rand
 
class Car:
    def __init__(self, model, year, color, name):
        self.model = model
        self.year = year
        self.color = color
        self.name = name
 
    def display(self):
        print("Name: ", self.name)
        print("Model:", self.model)
        print("Year:", self.year)
        print("Color:",self.color,"\n")
 
#data = []
# data.append(Car("Regular", 2017, "Grey", "Toyota"))
# data.append(Car("Special", 2019, "White", "BMV"))
# data.append(Car("Limited", 2016, "Green", "Honda"))


byte_stream = dill.dumps(Car)
print(byte_stream)

CarObject = dill.loads(byte_stream)
data = []

#CarObject("Regular", 2017, "Grey", "Toyota")
data.append(CarObject("Regular", 2017, "Grey", "Toyota"))
data.append(CarObject("Special", 2019, "White", "BMV"))
data.append(CarObject("Limited", 2016, "Green", "Honda"))

for x in data:
     x.display()