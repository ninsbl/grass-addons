MODULE_TOPDIR = ../../..

PGM = i.jaxa

SUBDIRS = jaxalib \
		i.jaxa.download \

include $(MODULE_TOPDIR)/include/Make/Dir.make

default: parsubdirs htmldir

install: installsubdirs
		$(INSTALL_DATA) $(PGM).html $(INST_DIR)/docs/html
