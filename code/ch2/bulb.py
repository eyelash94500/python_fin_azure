class Light_bulb:

    size = 10

    def __init__(self, color):
        # 初始化，設定初始值
        if color:
            self.color = color

    def open(self):
        print(f"開燈，燈是 {self.color} 色")

    def close(self):
        print("關燈")


red_light = Light_bulb("red")
print(red_light.size)
red_light.open()
red_light.close()

white_light = Light_bulb("white")
print(white_light.size)
white_light.open()
white_light.close()
