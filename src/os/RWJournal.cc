#include "RWJournal.h"
#include "NVMJournal.h"

RWJournal* RWJournal::create(string hot_journal, string cold_journal, string conf, BackStore* s, Finisher *f, int type)
{
    return new NVMJournal(hot_journal, cold_journal, conf, s, f);
}
