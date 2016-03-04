import config
import numpy as np
import sys

from indexEngine import IndexEngine
from query import QueryType
from querySender import QuerySender
from random import randint, random, seed, shuffle, uniform
from table import Table
from tableLoader import TableLoader

QUERIES_PER_DAY = 5000
RANDOM_PERCENTAGE_PER_DAY = 0.05
DAYS = 365
NOISE_FACTOR = 0.03


class Runner:

    def __init__(self, tableDirectory):
        self.currentDay = 1
        self.tableDirectory = tableDirectory

        self.tableLoader = TableLoader(tableDirectory, config.config["loadDumpedTables"])
        self.tableLoader.loadTables()

        self.querySender = QuerySender()
        self.indexEngine = IndexEngine()

        self.tables = []
        for tableName in self.tableLoader.getTableNames():
            self.tables.append(Table(self.tableDirectory, tableName))

        self.determineBoostInterval()
        self.periodActive = False
        self.queryDistributionStatistic = [[0] * DAYS for i in range(len(QueryType))]

    def addPeriodicQueries(self, queries):
        period = config.config["weeklyPeriodicQuery"]

        if self.currentDay >= period["startingFromDay"]:
            if (self.currentDay % 7) == period["startDay"]:
                self.periodActive = True
                self.remainingPeriodDuration = period["duration"]

            if self.periodActive:
                self.remainingPeriodDuration -= 1
                if self.remainingPeriodDuration == 0:
                    self.periodActive = False
                for i in range(int(QUERIES_PER_DAY * period["queryPercentage"])):
                    queries.append(self.tables[period["tableIndex"]].mediumQuery)
                print "Added %i medium queries of table with index %i and name %s" % (int(QUERIES_PER_DAY * period["queryPercentage"]), period["tableIndex"], self.tables[period["tableIndex"]].name)

                return int(QUERIES_PER_DAY * period["queryPercentage"])

        return 0

    def addRandomQueries(self, numberOfQueries, queries):
        for i in range(numberOfQueries):
            randomTable = randint(0, len(self.tables) - 1)
            randomQuery = randint(0, config.config["randomQueriesPerTable"] - 1)
            queries.append(self.tables[randomTable].randomQueries[randomQuery])

    def boostTableShares(self, tableShares, queriesToday):
        if self.currentDay % self.boostInterval == 1:
            self.determineBoostTables()
            self.determineBoostInterval()

        for boostIndex, value in zip(self.boostTables, config.config["boostValues"]):
            tableShares[boostIndex] = int(tableShares[boostIndex] + value * queriesToday)

    def determineBoostInterval(self):
        self.boostInterval = self.currentDay + randint(config.config["minBoostInterval"], config.config["maxBoostInterval"])

    def determineBoostTables(self):
        self.boostTables = []
        tables = sorted(range(len(self.tables)), key =lambda *args: random())

        for i in range(len(self.tables)):
            self.boostTables.append(tables[i])

    def noiseNumberOfQueries(self, numberOfQueries):
        multiplier = uniform(-NOISE_FACTOR, NOISE_FACTOR)

        return int(numberOfQueries * (1 + multiplier))

    def noiseTableShares(self, tableShares):
        multipliers = []
        numberOfMultipliers = len(self.tables) / 2 if len(self.tables) % 2 == 0 else len(self.tables) / 2 + 1
        for i in range(numberOfMultipliers):
            multipliers.append(uniform(-NOISE_FACTOR, NOISE_FACTOR))
            multipliers.append(multipliers[-1] * -1)

        tableShares = map(lambda tableShare, multiplier: int(tableShare * (1 + multiplier)), tableShares, multipliers[:len(tableShares)])
        return tableShares

    def prepareDay(self):
        queriesToday = self.noiseNumberOfQueries(QUERIES_PER_DAY)
        randomQueriesToday = int(queriesToday * RANDOM_PERCENTAGE_PER_DAY)
        sharedUsualQueries = 1.0 - RANDOM_PERCENTAGE_PER_DAY - reduce(lambda x, y: x + y, config.config["boostValues"])
        usualQueries = int(queriesToday * sharedUsualQueries)

        tableShares = [usualQueries / len(self.tables)] * len(self.tables)
        tableShares = self.noiseTableShares(tableShares)
        # print reduce(lambda x,y: x + y, tableShares)
        self.boostTableShares(tableShares, queriesToday)

        # print reduce(lambda x,y: x + y, tableShares)

        queries = self.prepareQueries(tableShares)

        periodicQueriesToday = self.addPeriodicQueries(queries)


        self.addRandomQueries(randomQueriesToday, queries)
        print "Sending %i queries today. Random: %i Periodic: %i" % (len(queries), randomQueriesToday, periodicQueriesToday)

        shuffle(queries)

        self.querySender.sendQueries(queries)

        if config.config["selfTunedIndexSelection"]:
            self.indexEngine.triggerIndexOptimization()
        else:
            if self.currentDay in [1, 2, 90, 180, 270]:
                self.indexEngine.triggerIndexOptimization()
            else:
                self.indexEngine.triggerConsolidation()

        self.queryDistributionStatistic[QueryType.random.value][self.currentDay - 1] = (randomQueriesToday)
        self.queryDistributionStatistic[QueryType.medium.value][self.currentDay - 1] += periodicQueriesToday


        self.currentDay += 1
        sys.stdout.flush()

    def prepareQueries(self, tableShares):
        queries = []
        for tableShare, table in zip(tableShares, self.tables):
            queries += [table.smallQuery] * (int(tableShare * config.config["queryShare"][QueryType.small.value]))
            queries += [table.mediumQuery] * (int(tableShare * config.config["queryShare"][QueryType.medium.value]))
            queries += [table.largeQuery] * (int(tableShare * config.config["queryShare"][QueryType.large.value]))

            self.queryDistributionStatistic[QueryType.small.value][self.currentDay - 1] += int(tableShare * config.config["queryShare"][QueryType.small.value])
            self.queryDistributionStatistic[QueryType.medium.value][self.currentDay - 1] += int(tableShare * config.config["queryShare"][QueryType.medium.value])
            self.queryDistributionStatistic[QueryType.large.value][self.currentDay - 1] += int(tableShare * config.config["queryShare"][QueryType.large.value])

        return queries

# For testing purposes, uncomment for random tables
seed(1238585430324)

if len(sys.argv) <> 2:
    print "Usage: python benchmark.py tableDirectory"
    sys.exit()
else:
    runner = Runner(sys.argv[1])

for i in range(DAYS):
    runner.prepareDay()

print "averageDayLength = %f" % (np.mean(runner.querySender.dayLengths))
print "totalDailyTimes = %s" % (runner.querySender.dayTimes)
print "queryStatistics = %s" % (runner.querySender.statistics)
print "indexOptimizationTimes = %s" % (runner.indexEngine.optimizationTimes)
print "queryDistribution = %s" % (runner.queryDistributionStatistic)