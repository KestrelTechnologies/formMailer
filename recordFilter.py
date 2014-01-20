#!/usr/bin/env python
'''
Project: form mailer
Author: Spencer Rathbun
Date: 1/19/2014
'''
import logging
class recordFilter(object):
    """This class hooks in the necessary filters for the records produced by the collecter in preparation for insertion into the DB."""

    def __init__(self, source, taxType, **kwargs):
        self.logger = logging.getLogger('formMailer.recordFilter')
        self.peoplefile = kwargs.pop('peoplefile', '')
        self.noPrintFile = kwargs.pop('noPrintFile', '')
        self.noMailFile = kwargs.pop('printNoMailFile', '')
        self.recInfo = {'total':0, 'totalOutput':0, 'notPrinted':0, 'suppressed':0, 'notMailed':0, 'noAddress':0}
        for key, value in kwargs.iteritems():
            self.logger.debug('Unknown key {0} with value {1}'.format(str(key), str(value)))
        self.logger.debug('New record filter inputs: {0}'.format(locals()))
        self.filter = self.filterRecs(source)

    def __iter__(self):
        return self

    def next(self):
        """Define next method to allow iteration."""
        return self.filter.next()

    def appendFields(self, sourceDicts):
        """Append fields with info."""
        for sourceDict in sourceDicts:
            if 'name' in sourceDict:
                sourceDict['BC_SPRING'] = sourceDict['Barcode_First_Installment'].strip('*')
                sourceDict['BC_FALL'] = sourceDict['Barcode_Second_Installment'].strip('*')
            sourceDict['print?'] = 'Y'
            sourceDict['mail?'] = 'Y'
            yield sourceDict

    def doNotPrintRecs(self, sourceDicts):
        """Filter input dicts against list of recs to not print."""
        try:
            noPrintList = []
            with open(self.noPrintFile, 'rb') as f:
                for line in f.readlines():
                    noPrintList.append(line.rstrip('\r\n').upper())
        except Exception,e:
            self.logger.error(str(e))
            noPrintList = []
        for sourceDict in sourceDicts:
            if 'Property Number' in sourceDict and sourceDict['Property_Number'] in noPrintList:
                sourceDict['print?'] = 'N'
                sourceDict['mail?'] = 'N'
                self.recInfo['notPrinted'] += 1
                self.recInfo['total'] += 1
            yield sourceDict

    def suppressPeople(self, sourceDicts):
        """Do not print or mail the specified peoples."""
        try:
            peoples = []
            with open(self.peoplefile, 'rb') as f:
                for line in f.readlines():
                    peoples.append(line.rstrip('\r\n').upper())
        except Exception,e:
            self.logger.error(str(e))
            peoples = []

        for sourceDict in sourceDicts:
            if sourceDict["Owner's Name"].upper() not in peoples:
                self.recInfo['totalOutput'] += 1
                self.recInfo['total'] += 1
            else:
                sourceDict['print?'] = 'N'
                sourceDict['mail?'] = 'N'
                self.recInfo['suppressed'] += 1
                self.recInfo['total'] += 1
            yield sourceDict

    def filterRecs(self, source):
        """Take iterator as input and filter it, yielding the results."""
        currentDicts = self.appendFields(source)
        if self.noPrintFile:
            currentDicts =  self.doNotPrintRecs(currentDicts)
        if self.noMailFile:
            currentDicts =  self.printNoMailRecs(currentDicts)
        if self.peoplefile:
            currentDicts = self.suppressPeople(currentDicts)
        return currentDicts

