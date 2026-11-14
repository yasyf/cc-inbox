CREATE TABLE record_root_audience (source_seq INTEGER PRIMARY KEY, drive TEXT NOT NULL, kind TEXT NOT NULL, audience INTEGER NOT NULL);
CREATE INDEX record_root_audience_parent ON records(re);
CREATE INDEX record_root_audience_stream ON record_root_audience(drive, audience, source_seq);
CREATE INDEX record_root_audience_kind_stream ON record_root_audience(drive, audience, kind, source_seq);
CREATE TRIGGER record_root_audience_insert AFTER INSERT ON records
BEGIN
DELETE FROM record_root_audience WHERE source_seq IN (SELECT r.seq FROM records r WHERE r.seq = NEW.seq);
INSERT INTO record_root_audience (source_seq, drive, kind, audience) SELECT r.seq, r.drive, r.kind,
CASE WHEN EXISTS (SELECT 1 FROM json_each(CASE WHEN json_error_position(r.recipients) = 0 THEN r.recipients ELSE '[]' END) WHERE value IN ('root', 'main'))
OR EXISTS (SELECT 1 FROM records own WHERE own.seq = r.re AND own.lane IN ('root', 'main')) THEN 1 ELSE 0 END
+ CASE WHEN (r.recipients = '[]' OR EXISTS (SELECT 1 FROM json_each(CASE WHEN json_error_position(r.recipients) = 0 THEN r.recipients ELSE '[]' END) WHERE value = 'all'))
AND r.lane NOT IN ('root', 'main') AND r.source NOT IN ('hook:root', 'hook:main') THEN 2 ELSE 0 END
FROM records r WHERE r.seq = NEW.seq;
DELETE FROM record_root_audience WHERE source_seq IN (SELECT r.seq FROM records r WHERE r.re = NEW.seq AND r.seq != NEW.seq);
INSERT INTO record_root_audience (source_seq, drive, kind, audience) SELECT r.seq, r.drive, r.kind,
CASE WHEN EXISTS (SELECT 1 FROM json_each(CASE WHEN json_error_position(r.recipients) = 0 THEN r.recipients ELSE '[]' END) WHERE value IN ('root', 'main'))
OR EXISTS (SELECT 1 FROM records own WHERE own.seq = r.re AND own.lane IN ('root', 'main')) THEN 1 ELSE 0 END
+ CASE WHEN (r.recipients = '[]' OR EXISTS (SELECT 1 FROM json_each(CASE WHEN json_error_position(r.recipients) = 0 THEN r.recipients ELSE '[]' END) WHERE value = 'all'))
AND r.lane NOT IN ('root', 'main') AND r.source NOT IN ('hook:root', 'hook:main') THEN 2 ELSE 0 END
FROM records r WHERE r.re = NEW.seq AND r.seq != NEW.seq;
END;
CREATE TRIGGER record_root_audience_update AFTER UPDATE OF seq, drive, kind, lane, source, recipients, re ON records
WHEN OLD.seq IS NOT NEW.seq OR OLD.drive IS NOT NEW.drive OR OLD.kind IS NOT NEW.kind OR OLD.lane IS NOT NEW.lane OR OLD.source IS NOT NEW.source OR OLD.recipients IS NOT NEW.recipients OR OLD.re IS NOT NEW.re
BEGIN
DELETE FROM record_root_audience WHERE source_seq = OLD.seq;
DELETE FROM record_root_audience WHERE source_seq IN (SELECT r.seq FROM records r WHERE r.seq = NEW.seq);
INSERT INTO record_root_audience (source_seq, drive, kind, audience) SELECT r.seq, r.drive, r.kind,
CASE WHEN EXISTS (SELECT 1 FROM json_each(CASE WHEN json_error_position(r.recipients) = 0 THEN r.recipients ELSE '[]' END) WHERE value IN ('root', 'main'))
OR EXISTS (SELECT 1 FROM records own WHERE own.seq = r.re AND own.lane IN ('root', 'main')) THEN 1 ELSE 0 END
+ CASE WHEN (r.recipients = '[]' OR EXISTS (SELECT 1 FROM json_each(CASE WHEN json_error_position(r.recipients) = 0 THEN r.recipients ELSE '[]' END) WHERE value = 'all'))
AND r.lane NOT IN ('root', 'main') AND r.source NOT IN ('hook:root', 'hook:main') THEN 2 ELSE 0 END
FROM records r WHERE r.seq = NEW.seq;
DELETE FROM record_root_audience WHERE source_seq IN (SELECT r.seq FROM records r WHERE r.re IN (OLD.seq, NEW.seq) AND r.seq != NEW.seq AND (OLD.seq IS NOT NEW.seq OR OLD.lane IS NOT NEW.lane));
INSERT INTO record_root_audience (source_seq, drive, kind, audience) SELECT r.seq, r.drive, r.kind,
CASE WHEN EXISTS (SELECT 1 FROM json_each(CASE WHEN json_error_position(r.recipients) = 0 THEN r.recipients ELSE '[]' END) WHERE value IN ('root', 'main'))
OR EXISTS (SELECT 1 FROM records own WHERE own.seq = r.re AND own.lane IN ('root', 'main')) THEN 1 ELSE 0 END
+ CASE WHEN (r.recipients = '[]' OR EXISTS (SELECT 1 FROM json_each(CASE WHEN json_error_position(r.recipients) = 0 THEN r.recipients ELSE '[]' END) WHERE value = 'all'))
AND r.lane NOT IN ('root', 'main') AND r.source NOT IN ('hook:root', 'hook:main') THEN 2 ELSE 0 END
FROM records r WHERE r.re IN (OLD.seq, NEW.seq) AND r.seq != NEW.seq AND (OLD.seq IS NOT NEW.seq OR OLD.lane IS NOT NEW.lane);
END;
CREATE TRIGGER record_root_audience_delete AFTER DELETE ON records
BEGIN
DELETE FROM record_root_audience WHERE source_seq = OLD.seq;
DELETE FROM record_root_audience WHERE source_seq IN (SELECT r.seq FROM records r WHERE r.re = OLD.seq);
INSERT INTO record_root_audience (source_seq, drive, kind, audience) SELECT r.seq, r.drive, r.kind,
CASE WHEN EXISTS (SELECT 1 FROM json_each(CASE WHEN json_error_position(r.recipients) = 0 THEN r.recipients ELSE '[]' END) WHERE value IN ('root', 'main'))
OR EXISTS (SELECT 1 FROM records own WHERE own.seq = r.re AND own.lane IN ('root', 'main')) THEN 1 ELSE 0 END
+ CASE WHEN (r.recipients = '[]' OR EXISTS (SELECT 1 FROM json_each(CASE WHEN json_error_position(r.recipients) = 0 THEN r.recipients ELSE '[]' END) WHERE value = 'all'))
AND r.lane NOT IN ('root', 'main') AND r.source NOT IN ('hook:root', 'hook:main') THEN 2 ELSE 0 END
FROM records r WHERE r.re = OLD.seq;
END;
