PYTHON ?= python3

.PHONY: start add delete pause resume killall clean

start killall clean:
	@$(PYTHON) "$(CURDIR)/playlistctl.py" $@

add delete:
	@$(PYTHON) "$(CURDIR)/playlistctl.py" $@ "$(NODE)" "$(SONG)"

pause resume:
	@$(PYTHON) "$(CURDIR)/playlistctl.py" $@ "$(NODE)"
