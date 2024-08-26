import flet as ft
import flet_material as fm

def main(page: ft.Page):

    page.bgcolor = fm.Theme.bgcolor

    annotation = fm.Annotations("This is an annotated message!")

    page.add(annotation)

    page.update()


if __name__ == "__main__":
    ft.app(target=main)
