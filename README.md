# LeanITTest
Проект собирается SBT (в т.ч. assembly) и запускается командой
spark-submit --class ru.noname.Solution --master local[*] lean-it-assembly-0.1.jar inputPath outputPath
, где local[*] написал для примера, inputPath - путь к директории hdfs, содержащей  ROUTES.csv и ouputPath - путь для сохранения результатов.
