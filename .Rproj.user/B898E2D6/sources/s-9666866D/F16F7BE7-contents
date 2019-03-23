CREATE CONSTRAINT ON (c:Country) ASSERT exists(c.name);
CREATE CONSTRAINT ON (c:Country) ASSERT exists(c.code);
CREATE CONSTRAINT ON (c:Country) ASSERT c.name IS UNIQUE;
CREATE CONSTRAINT ON (c:Country) ASSERT c.code IS UNIQUE;

CREATE CONSTRAINT ON (m:Migration) ASSERT exists(m.total);
CREATE CONSTRAINT ON (m:Migration) ASSERT exists(m.male);
CREATE CONSTRAINT ON (m:Migration) ASSERT exists(m.female);

CREATE CONSTRAINT ON (y:Year) ASSERT exists(y.year);
CREATE CONSTRAINT ON (y:Year) ASSERT y.year IS UNIQUE;

CREATE CONSTRAINT ON (i:Income) ASSERT exists(i.code);
CREATE CONSTRAINT ON (i:Income) ASSERT exists(i.name);
CREATE CONSTRAINT ON (i:Income) ASSERT i.code IS UNIQUE;
CREATE CONSTRAINT ON (i:Income) ASSERT i.name IS UNIQUE;

CREATE CONSTRAINT ON (r:Region) ASSERT exists(r.code);
CREATE CONSTRAINT ON (r:Region) ASSERT exists(r.name);
CREATE CONSTRAINT ON (r:Region) ASSERT r.code IS UNIQUE;
CREATE CONSTRAINT ON (r:Region) ASSERT r.name IS UNIQUE;