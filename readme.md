## Инструменты для приведения слоёв гидрографии к гидрологически корректному виду

В тулбоксе river_tracing находятся инструменты, позволяющие автоматизировать приведение слоёв гидрографии из картографических наборов данных (например, VMAP0) к гидрологически корректному виду, т.е. заменить все полигональные водные объекты на линейные путём выделения центральных линий, разбить линии в местах слияния, обеспечить древовидность гидрографической сети, а также согласовать направление трассировки линий с направлениями течения соответствующих им водотоков.

Список инструментов:
1. Voronoi Skeleton. Выделение скелетов полигонов с помощью диаграммы Вороного.
2. Voronoi Skeleton Batch. Выделение скелетов полигонов с помощью диаграммы Вороного с разбиением на группы (для больших объёмов данных)
3. Prepare Rivers. Подготовка линейных водных объектов к трассировке после слияния со скелетами полигонов.
4. Trace Rivers. Трассировка маршрутов по речной сети от истоков к устьям и выделение итоговой гидрографической сети из маршрутных линий.
5. Trace Rivers by Basin. Трассировка маршрутов по речной сети от истоков к устьям и выделение итоговой гидрографической сети из маршрутных линий. Трассировка производится в пределах границ водосборных бассейнов, указанных пользователем.

В папке `data/` находится набор данных, подготовленный на основе VMAP0 и Цифровой географической основы ВСЕГЕИ (на территорию Азиатской части России). 