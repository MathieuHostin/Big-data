# Projet Pi-estimator : Antoine et Mathieu GOTY

:point_right: Création du code sur IDLE

![](https://github.com/MathieuHostin/Big-data/blob/main/Pi-estimator/Image/IDLE.PNG)
*Capture d'écran n°1 : Code effectué*


:point_right: Ouverture de l'invite de commandes Windows afin de lancer le code

:point_right: Commande spark-submit *nom_du_projet*

![](https://github.com/MathieuHostin/Big-data/blob/main/Pi-estimator/Image/Commande%20de%20lancement.PNG)
*Capture d'écran n°2 : Commande*

:point_right: Obtention du résultat Pi pour la méthode avec Spark et celle avec Numpy et de leur temps d'execution 

![](https://github.com/MathieuHostin/Big-data/blob/main/Pi-estimator/Image/Output.PNG)
*Capture d'écran n°3 : Résultat*


:point_right: Tableau récapitulatif de l'estimation de pi et des temps d'exécutions

|    n = 100000     |  Spark   |   numpy  |
|-------------------|----------|----------|
| Temps d'exécution |  9.813s  |  0.014s  |
|   Valeurs de pi   | 3.137480 | 3.132760 |
|  écart % Math.pi  |  -0.24%  |  -0.40%  |
