-- Funktion zur Konvertierung von docfilename zu Minuten seit Referenzdatum
CREATE OR REPLACE FUNCTION public.nifix_convert_docfilename2date(
    docfilename TEXT
)
RETURNS BIGINT
LANGUAGE 'plpgsql'
COST 100
IMMUTABLE PARALLEL UNSAFE
AS $BODY$
DECLARE
    scan_year INTEGER;
    scan_month INTEGER;
    scan_day INTEGER;
    scan_hour INTEGER DEFAULT 0;
    scan_minute INTEGER DEFAULT 0;

    ref_date DATE := '1600-01-01';
    calculated_date DATE;
    days_since_ref INTEGER;
BEGIN
    -- Format: YYYY_MM_DD_HH_MM_SS.pdf oder Buchstaben vor YYYY (z. B. AB2024_11_23_14_30_00.pdf)
    IF docfilename ~ '^[A-Za-z]{0,2}?\d{4}_\d{2}_\d{2}_\d{2}_\d{2}_\d{2}\.pdf$' THEN
        scan_year := substring(docfilename FROM '^[A-Za-z]{0,2}?(\d{4})')::int;
        scan_month := split_part(docfilename, '_', 2)::int;
        scan_day := split_part(docfilename, '_', 3)::int;
        scan_hour := split_part(docfilename, '_', 4)::int;
        scan_minute := split_part(docfilename, '_', 5)::int;

    -- Format: Arzt-DDMMYYYY*.pdf oder Arzt_DDMMYYYY*.pdf
    ELSIF docfilename ~ '^Arzt[-_]\d{2}\d{2}\d{4}.*\.pdf$' THEN
        scan_day := substring(docfilename FROM 'Arzt[-_](\d{2})')::int;
        scan_month := substring(docfilename FROM 'Arzt[-_]\d{2}(\d{2})')::int;
        scan_year := substring(docfilename FROM 'Arzt[-_]\d{2}\d{2}(\d{4})')::int;
        scan_hour := 12;
        scan_minute := 0;

    -- Format: Scan_YYYY-MM-DD_HHMMSS.pdf
    ELSIF docfilename ~ '^Scan_\d{4}-\d{2}-\d{2}_\d{6}\.pdf$' THEN
        scan_year := substring(docfilename FROM 'Scan_(\d{4})-')::int;
        scan_month := substring(docfilename FROM 'Scan_\d{4}-(\d{2})-')::int;
        scan_day := substring(docfilename FROM 'Scan_\d{4}-\d{2}-(\d{2})')::int;
        scan_hour := substring(docfilename FROM '_(\d{2})\d{4}')::int;
        scan_minute := substring(docfilename FROM '_\d{2}(\d{2})\d{2}')::int;

    -- Format: Scan_Arzt_YYYY-MM-DD_HHMMSS.pdf
    ELSIF docfilename ~ '^Scan_Arzt_\d{4}-\d{2}-\d{2}_\d{6}\.pdf$' THEN
        scan_year := substring(docfilename FROM 'Scan_Arzt_(\d{4})-')::int;
        scan_month := substring(docfilename FROM 'Scan_Arzt_\d{4}-(\d{2})-')::int;
        scan_day := substring(docfilename FROM 'Scan_Arzt_\d{4}-\d{2}-(\d{2})')::int;
        scan_hour := substring(docfilename FROM '_(\d{2})\d{4}')::int;
        scan_minute := substring(docfilename FROM '_\d{2}(\d{2})\d{2}')::int;

    ELSE
        -- Debugging-Logik
        -- RAISE NOTICE 'Ungültiges Dateiformat: %', docfilename;
        RETURN NULL;
    END IF;

    -- Berechnung der Differenz in Tagen und Minuten
    calculated_date := make_date(scan_year, scan_month, scan_day);
    days_since_ref := calculated_date - ref_date;

    -- Rückgabe: Minuten seit dem Referenzdatum
    RETURN (days_since_ref + 1) * 1440 + scan_hour * 60 + scan_minute;
END;
$BODY$;

-- Funktion zur automatischen Aktualisierung von docdate basierend auf docfilename
CREATE OR REPLACE FUNCTION public.nifix_update_docs_date_trigger()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    newdate INTEGER;
BEGIN
    -- Überprüfe, ob doctype = 1 ist und docfilename nicht NULL
    IF NEW.doctype = 1 AND COALESCE(NEW.docfilename, '') <> '' THEN
        -- Berechne das Datum basierend auf docfilename
        newdate := public.nifix_convert_docfilename2date(NEW.docfilename);

        -- Falls ein gültiges Datum berechnet wurde, setze es in docdate
        IF newdate IS NOT NULL THEN
            NEW.docdate := newdate;
        ELSE
            -- Optional: Logge einen Hinweis bei ungültigem Dateiformat
            -- RAISE NOTICE 'Ungültiges Dateiformat: %', NEW.docfilename;
        END IF;
    END IF;

    RETURN NEW;
END;
$BODY$;

ALTER FUNCTION public.nifix_update_docs_date_trigger()
    OWNER TO postgres;

-- Dynamische Erstellung von Triggern für mehrere Tabellen
DO $$
DECLARE
    tablename TEXT;
    triggername TEXT;
BEGIN
    -- Liste der Tabellen
    FOR tablename IN
        SELECT unnest(ARRAY[
            'patientdocs',
            'addressdocs',
            'herstellerdocs',
            'materialdocs',
            'fremdlabordocs',
            'geraetedocs',
            'userdocs',
            'fortbildungdocs'
        ])
    LOOP
        -- Generiere eindeutigen Trigger-Namen
        triggername := 'nifix_set_' || tablename || '_date_on_insert';

        -- Existierenden Trigger löschen, falls vorhanden, und neuen erstellen
        EXECUTE format($f$
            DROP TRIGGER IF EXISTS %I ON %I;
            CREATE TRIGGER %I
            BEFORE INSERT
            ON %I
            FOR EACH ROW
            EXECUTE FUNCTION public.nifix_update_docs_date_trigger();
        $f$, triggername, tablename, triggername, tablename);
    END LOOP;

    -- Aktualisiere vorhandene Einträge in jeder Tabelle
    FOR tablename IN
        SELECT unnest(ARRAY[
            'patientdocs',
            'addressdocs',
            'herstellerdocs',
            'materialdocs',
            'fremdlabordocs',
            'geraetedocs',
            'userdocs',
            'fortbildungdocs'
        ])
    LOOP
        EXECUTE format($f$
            UPDATE %I
            SET docdate = public.nifix_convert_docfilename2date(docfilename)
            WHERE public.nifix_convert_docfilename2date(docfilename) IS NOT NULL;
        $f$, tablename);
    END LOOP;
END $$;
