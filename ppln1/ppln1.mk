.PHONY: all dbclean

SHELL = /bin/bash
DMODE ?= client

dbclean.log: 
	$(SHELL) -x -f submit.sh job0=dbclean xargs=delete 2>&1 | tee $@

X_JOBS ?= state pi raw0 cv1 lda2

define PROGRAM_template =
all:: $(1).log

ALL_LOGS += $(1).log

$(1).log:
	$(SHELL) -x -f submit.sh dmode=$(DMODE) job0=$(1) 2>&1 | tee $(1).log
endef

$(foreach prog,$(X_JOBS),$(eval $(call PROGRAM_template,$(prog))))

clean:
	$(RM) -f $(ALL_LOGS)

