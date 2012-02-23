EBOOKS = 2814 4217 4300

BOOK_FILES = $(foreach ebook,$(EBOOKS),$(ebook).txt)
MTBL_FILES = $(foreach ebook,$(EBOOKS),$(ebook).mtbl)

%.txt:
	@wget -O $@ http://www.gutenberg.org/files/$(shell basename $@ .txt)/$@

%.mtbl: %.txt
	rm -f $@
	./pymtbl_wf_analyze.py $< $@

wf.txt: wf.mtbl $(BOOK_FILES)
	./pymtbl_wf_dump.py wf.mtbl | sort -rn > wf.txt

wf.mtbl: $(MTBL_FILES) $(BOOK_FILES)
	rm -f wf.mtbl
	./pymtbl_wf_merge.py $(MTBL_FILES) wf.mtbl

clean:
	rm -f wf.mtbl wf.txt $(MTBL_FILES)

.PHONY: clean
