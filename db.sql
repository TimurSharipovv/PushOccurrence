--
-- PostgreSQL database dump
--

-- Dumped from database version 17.5
-- Dumped by pg_dump version 17.5

-- Started on 2026-02-02 18:07:53

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 6 (class 2615 OID 16411)
-- Name: data_exchange; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA data_exchange;


ALTER SCHEMA data_exchange OWNER TO postgres;

--
-- TOC entry 223 (class 1255 OID 24612)
-- Name: log_changes(); Type: FUNCTION; Schema: data_exchange; Owner: postgres
--

CREATE FUNCTION data_exchange.log_changes() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_id uuid;
BEGIN
    INSERT INTO data_exchange.message_queue_log (message_type)
    VALUES (TG_OP)
    RETURNING message_id INTO v_id;

    INSERT INTO data_exchange.message_queue_log_data (message_id, message_body)
    VALUES (v_id, row_to_json(NEW));

    PERFORM pg_notify('queue_message_log', v_id::text);

    RETURN NEW;
END;
$$;


ALTER FUNCTION data_exchange.log_changes() OWNER TO postgres;

--
-- TOC entry 222 (class 1255 OID 16432)
-- Name: log_user_changes(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.log_user_changes() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        INSERT INTO data_exchange.message_queue_log (table_name, operation, payload)
        VALUES (TG_TABLE_NAME, TG_OP, row_to_json(OLD));
    ELSE
        INSERT INTO data_exchange.message_queue_log (table_name, operation, payload)
        VALUES (TG_TABLE_NAME, TG_OP, row_to_json(NEW));
    END IF;

    PERFORM pg_notify('queue_message_log', (SELECT id::text FROM data_exchange.message_queue_log ORDER BY created_at DESC LIMIT 1));
    RETURN NEW;
END;
$$;


ALTER FUNCTION public.log_user_changes() OWNER TO postgres;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 220 (class 1259 OID 24591)
-- Name: message_queue_log; Type: TABLE; Schema: data_exchange; Owner: postgres
--

CREATE TABLE data_exchange.message_queue_log (
    message_id uuid DEFAULT gen_random_uuid() NOT NULL,
    message_time timestamp with time zone DEFAULT now(),
    message_type text NOT NULL,
    transferred boolean DEFAULT false,
    transfer_time timestamp with time zone
);


ALTER TABLE data_exchange.message_queue_log OWNER TO postgres;

--
-- TOC entry 221 (class 1259 OID 24601)
-- Name: message_queue_log_data; Type: TABLE; Schema: data_exchange; Owner: postgres
--

CREATE TABLE data_exchange.message_queue_log_data (
    message_id uuid NOT NULL,
    message_time timestamp with time zone DEFAULT now(),
    message_body jsonb NOT NULL
);


ALTER TABLE data_exchange.message_queue_log_data OWNER TO postgres;

--
-- TOC entry 219 (class 1259 OID 16435)
-- Name: users; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.users (
    id integer NOT NULL,
    name text,
    email text
);


ALTER TABLE public.users OWNER TO postgres;

--
-- TOC entry 218 (class 1259 OID 16434)
-- Name: users_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.users_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.users_id_seq OWNER TO postgres;

--
-- TOC entry 4818 (class 0 OID 0)
-- Dependencies: 218
-- Name: users_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.users_id_seq OWNED BY public.users.id;


--
-- TOC entry 4652 (class 2604 OID 16438)
-- Name: users id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.users ALTER COLUMN id SET DEFAULT nextval('public.users_id_seq'::regclass);


--
-- TOC entry 4811 (class 0 OID 24591)
-- Dependencies: 220
-- Data for Name: message_queue_log; Type: TABLE DATA; Schema: data_exchange; Owner: postgres
--

COPY data_exchange.message_queue_log (message_id, message_time, message_type, transferred, transfer_time) FROM stdin;
8f3efe0b-2973-4415-aa1e-c4056138e523	2025-11-17 10:47:01.535804+03	INSERT users	t	2025-11-17 10:55:49.463293+03
079f7cf5-3805-432d-8094-502213f116fa	2025-11-17 11:08:46.32633+03	INSERT	t	2025-11-17 11:08:46.331936+03
7019520a-b0d3-48e3-90cd-98f764465a08	2025-11-17 12:59:02.831134+03	INSERT	t	2025-11-17 13:43:51.22646+03
638e423a-9d5c-4527-9701-74a17b433e4b	2025-11-17 12:59:02.978326+03	INSERT	t	2025-11-17 13:43:51.229099+03
7383bda6-6a92-4b29-9e24-f7796d79186b	2025-11-17 12:59:10.012898+03	INSERT	t	2025-11-17 13:43:51.230746+03
6f5b907d-fd5c-4d91-a389-8b6dc082aa2f	2025-11-17 13:05:10.284053+03	INSERT	t	2025-11-17 13:43:51.233969+03
ac4d60ec-ab6c-41ae-a2d0-a788b812dbd5	2025-11-17 10:44:44.910698+03	INSERT users	t	2025-11-17 16:12:23.22814+03
4811b360-9305-4a8c-a303-b7fd339d2e50	2026-01-12 14:40:47.48217+03	INSERT	t	2026-01-12 14:40:47.735659+03
59f6603d-f93f-48bc-82fe-1080189609ab	2026-01-12 14:59:04.27771+03	INSERT	t	2026-01-12 14:59:04.46037+03
f8851bbe-4d3f-4e66-81b9-7c51451fb79a	2026-01-12 15:19:51.457649+03	INSERT	t	2026-01-12 15:19:51.726345+03
b003ec2b-b713-408e-8cc6-6e6f1030c66d	2026-01-12 17:04:05.392388+03	INSERT	t	2026-01-12 17:04:05.612997+03
1fe0beb6-992f-4322-ad62-9d6be1b48bf0	2025-11-17 16:12:48.162961+03	INSERT	t	2026-01-12 17:16:38.463811+03
2d7f40a4-2757-4713-8f67-3849dc169d6d	2025-11-17 16:20:40.921946+03	INSERT	t	2026-01-12 17:16:38.469608+03
2a403a2b-524a-4942-98c0-7ac49f13856b	2025-11-17 16:38:50.741914+03	INSERT	t	2026-01-12 17:16:38.470444+03
a0f7bc84-8bdd-4f21-841d-ba30b82c65dc	2026-01-12 12:32:15.282235+03	INSERT	t	2026-01-12 17:16:38.472366+03
22b3d2ba-7c2f-46c8-a176-40d6c903e0c7	2026-01-12 11:34:14.342824+03	INSERT	t	2026-01-12 17:16:38.472384+03
7c3db5ea-c385-4ba2-ae39-0a423604b567	2026-01-12 13:36:24.334067+03	INSERT	t	2026-01-12 17:16:38.481385+03
a6ca2124-b79a-4042-8639-7f4b722e8cc3	2026-01-12 14:13:20.953848+03	INSERT	t	2026-01-12 17:16:38.488577+03
ffff519e-7e5e-4bab-878a-568d2a73dc3e	2026-01-12 14:26:45.124143+03	INSERT	t	2026-01-12 17:16:38.509135+03
36828fbf-82ac-447b-bc30-38285471053c	2026-01-12 16:47:56.181348+03	INSERT	t	2026-01-12 17:16:38.509583+03
f2fa03e1-05ad-4a1a-bd84-d0dae60030ac	2026-01-12 16:58:38.398415+03	INSERT	t	2026-01-12 17:16:38.512702+03
8c0fd092-ceae-4396-97f8-06895d949d0d	2026-01-12 17:00:04.696096+03	INSERT	t	2026-01-12 17:16:38.514924+03
8f7ab1c4-ceaf-4663-969a-31eccc2f52f1	2025-11-17 16:12:41.111153+03	INSERT	t	2026-01-12 17:16:38.520702+03
cd1848c4-739c-4041-a982-3e88429816c4	2026-01-12 17:20:15.174587+03	INSERT	t	2026-01-12 17:20:15.177916+03
bcf7b536-c127-4016-9bcb-d25cacf0678d	2026-01-12 17:20:32.574385+03	INSERT	t	2026-01-12 17:20:32.577963+03
1412eaf8-8be9-419e-92b0-ea04bdecb8b0	2026-01-12 17:31:36.547523+03	INSERT	f	\N
097463f0-2f4a-4066-8ecb-92aac218571b	2026-01-13 10:50:04.91175+03	INSERT	t	2026-01-13 10:50:05.122705+03
0129d5bf-3b01-4b87-9c09-37589fb7370b	2026-01-13 10:53:07.798672+03	INSERT	f	\N
0b99fc75-c8b5-43be-b4d4-f4e5b14e7bbb	2026-01-13 19:02:19.624578+03	INSERT	f	\N
c753feb4-38ad-49be-bb4b-90eb3597c920	2026-01-14 14:25:17.292906+03	INSERT	f	\N
70c4aa54-cb83-4447-83a4-5b2c6308ff8e	2026-01-14 14:26:50.598635+03	INSERT	f	\N
f07b6faf-b9bf-4895-8a9d-36c204b03ccd	2026-01-14 18:52:52.652523+03	INSERT	t	2026-01-14 18:52:52.86601+03
ce69f189-2d14-486a-81e0-8f7fe2810995	2026-01-15 12:47:46.892848+03	INSERT	t	2026-01-15 12:47:47.24171+03
\.


--
-- TOC entry 4812 (class 0 OID 24601)
-- Dependencies: 221
-- Data for Name: message_queue_log_data; Type: TABLE DATA; Schema: data_exchange; Owner: postgres
--

COPY data_exchange.message_queue_log_data (message_id, message_time, message_body) FROM stdin;
8f3efe0b-2973-4415-aa1e-c4056138e523	2025-11-17 10:49:34.020847+03	{"name": "Timur Updated", "action": "update", "user_id": 20}
8f3efe0b-2973-4415-aa1e-c4056138e523	2025-11-17 10:50:00.386032+03	{"name": "Timur Updated", "action": "update", "user_id": 20}
8f3efe0b-2973-4415-aa1e-c4056138e523	2025-11-17 11:03:52.889618+03	{"name": "Timur", "action": "insert", "user_id": 10}
8f3efe0b-2973-4415-aa1e-c4056138e523	2025-11-17 11:03:57.861645+03	{"name": "Timur", "action": "insert", "user_id": 10}
079f7cf5-3805-432d-8094-502213f116fa	2025-11-17 11:08:46.32633+03	{"id": 24, "name": "TestUser", "email": "test@example.com"}
7019520a-b0d3-48e3-90cd-98f764465a08	2025-11-17 12:59:02.831134+03	{"id": 25, "name": "TestUser", "email": "test@example.com"}
638e423a-9d5c-4527-9701-74a17b433e4b	2025-11-17 12:59:02.978326+03	{"id": 26, "name": "TestUser", "email": "test@example.com"}
7383bda6-6a92-4b29-9e24-f7796d79186b	2025-11-17 12:59:10.012898+03	{"id": 27, "name": "Tfvhgh", "email": "teghghst@example.com"}
6f5b907d-fd5c-4d91-a389-8b6dc082aa2f	2025-11-17 13:05:10.284053+03	{"id": 28, "name": "Tfvhgh", "email": "teghghst@example.com"}
8f7ab1c4-ceaf-4663-969a-31eccc2f52f1	2025-11-17 16:12:41.111153+03	{"id": 29, "name": "Tfvhjjjgh", "email": "teghghst@exajjjjmple.com"}
1fe0beb6-992f-4322-ad62-9d6be1b48bf0	2025-11-17 16:12:48.162961+03	{"id": 30, "name": "Tfvhjjjgh", "email": "teghghst@exajjjjmple.com"}
2d7f40a4-2757-4713-8f67-3849dc169d6d	2025-11-17 16:20:40.921946+03	{"id": 31, "name": "Tfvhjjjjhjjjjgh", "email": "teghgjjhjst@exajjjjmple.com"}
2a403a2b-524a-4942-98c0-7ac49f13856b	2025-11-17 16:38:50.741914+03	{"id": 32, "name": "Tfvhjjjjhjjjjgh", "email": "teghgjjhjst@exajjjjmple.com"}
22b3d2ba-7c2f-46c8-a176-40d6c903e0c7	2026-01-12 11:34:14.342824+03	{"id": 33, "name": "Tfvhjjllllljgh", "email": "teghgllllllljst@exajjjjmple.com"}
a0f7bc84-8bdd-4f21-841d-ba30b82c65dc	2026-01-12 12:32:15.282235+03	{"id": 34, "name": "Tfuuuuuuuugh", "email": "teghguuuuuuullljst@exajjjjmple.com"}
7c3db5ea-c385-4ba2-ae39-0a423604b567	2026-01-12 13:36:24.334067+03	{"id": 35, "name": "Tfuuuuvbnmgh", "email": "teghguzxcvbllljst@exajjjjmple.com"}
a6ca2124-b79a-4042-8639-7f4b722e8cc3	2026-01-12 14:13:20.953848+03	{"id": 36, "name": "Tfuuuuvbnmgh", "email": "teghguzxcvbllljst@exajjjjmple.com"}
ffff519e-7e5e-4bab-878a-568d2a73dc3e	2026-01-12 14:26:45.124143+03	{"id": 37, "name": "Tfuuutyhjghjhgjghjghjhgh", "email": "ubllljst@exajjjjmple.com"}
4811b360-9305-4a8c-a303-b7fd339d2e50	2026-01-12 14:40:47.48217+03	{"id": 38, "name": "Tppppphjghjhgjghjghjhgh", "email": "ubllpppppp@exajjjjmple.com"}
59f6603d-f93f-48bc-82fe-1080189609ab	2026-01-12 14:59:04.27771+03	{"id": 39, "name": "mmmmmhjhgjghjghjhgh", "email": "ummmmmmp@exajjjjmple.com"}
f8851bbe-4d3f-4e66-81b9-7c51451fb79a	2026-01-12 15:19:51.457649+03	{"id": 40, "name": "iiiiiii", "email": "iiiiii@example.com"}
36828fbf-82ac-447b-bc30-38285471053c	2026-01-12 16:47:56.181348+03	{"id": 41, "name": "iiiyiiii", "email": "iiiiyii@example.com"}
f2fa03e1-05ad-4a1a-bd84-d0dae60030ac	2026-01-12 16:58:38.398415+03	{"id": 42, "name": "iiiyiiii", "email": "iiiiyii@example.com"}
8c0fd092-ceae-4396-97f8-06895d949d0d	2026-01-12 17:00:04.696096+03	{"id": 43, "name": "iiiyiiii", "email": "iiiiyii@example.com"}
b003ec2b-b713-408e-8cc6-6e6f1030c66d	2026-01-12 17:04:05.392388+03	{"id": 44, "name": "iiiyiiii", "email": "iiiiyii@example.com"}
cd1848c4-739c-4041-a982-3e88429816c4	2026-01-12 17:20:15.174587+03	{"id": 45, "name": "iiiggyiiii", "email": "iiiiggyii@example.com"}
bcf7b536-c127-4016-9bcb-d25cacf0678d	2026-01-12 17:20:32.574385+03	{"id": 46, "name": "iiiggyiiii", "email": "iiiiggyii@example.com"}
1412eaf8-8be9-419e-92b0-ea04bdecb8b0	2026-01-12 17:31:36.547523+03	{"id": 47, "name": "iiihhggyiiii", "email": "iiihhiggyii@example.com"}
097463f0-2f4a-4066-8ecb-92aac218571b	2026-01-13 10:50:04.91175+03	{"id": 48, "name": "iiiggyiiii", "email": "iiiiggyii@example.com"}
0129d5bf-3b01-4b87-9c09-37589fb7370b	2026-01-13 10:53:07.798672+03	{"id": 49, "name": "iighghiughgyiiii", "email": "iiighghghghghghyii@example.com"}
0b99fc75-c8b5-43be-b4d4-f4e5b14e7bbb	2026-01-13 19:02:19.624578+03	{"id": 50, "name": "iiрррbbbb", "email": "bрррb@example.com"}
c753feb4-38ad-49be-bb4b-90eb3597c920	2026-01-14 14:25:17.292906+03	{"id": 51, "name": "iizzz", "email": "bzzzzb@example.com"}
70c4aa54-cb83-4447-83a4-5b2c6308ff8e	2026-01-14 14:26:50.598635+03	{"id": 52, "name": "iizxzxzxxzyiiii", "email": "iizxxzxziiggyii@example.com"}
f07b6faf-b9bf-4895-8a9d-36c204b03ccd	2026-01-14 18:52:52.652523+03	{"id": 53, "name": "iizxppppxzyiiii", "email": "iizpppzxziiggyii@example.com"}
ce69f189-2d14-486a-81e0-8f7fe2810995	2026-01-15 12:47:46.892848+03	{"id": 54, "name": "iiqqqxzyiiii", "email": "iizppqqziiggyii@example.com"}
\.


--
-- TOC entry 4810 (class 0 OID 16435)
-- Dependencies: 219
-- Data for Name: users; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.users (id, name, email) FROM stdin;
1	Test	t@example.com
2	Alice	a@example.com
3	Alice	a@example.com
4	Timur	a@example.com
5	Alice	a@example.com
6	Test	t@example.com
7	test2222	a@examp456546le.com
8	test2222	a@examp456546le.com
9	test2222	a@examp456546le.com
12	test123456	test@123456.com
13	test123456	test@123456.com
14	Bob	b@example.com
15	Bob	b@example.com
16	Bob	b@example.com
17	Bob	b@example.com
18	Bob	b@example.com
19	Bottteb	b@example.com
20	Bottteb	b@example.com
21	Bottteb	b@example.com
24	TestUser	test@example.com
25	TestUser	test@example.com
26	TestUser	test@example.com
27	Tfvhgh	teghghst@example.com
28	Tfvhgh	teghghst@example.com
29	Tfvhjjjgh	teghghst@exajjjjmple.com
30	Tfvhjjjgh	teghghst@exajjjjmple.com
31	Tfvhjjjjhjjjjgh	teghgjjhjst@exajjjjmple.com
32	Tfvhjjjjhjjjjgh	teghgjjhjst@exajjjjmple.com
33	Tfvhjjllllljgh	teghgllllllljst@exajjjjmple.com
34	Tfuuuuuuuugh	teghguuuuuuullljst@exajjjjmple.com
35	Tfuuuuvbnmgh	teghguzxcvbllljst@exajjjjmple.com
36	Tfuuuuvbnmgh	teghguzxcvbllljst@exajjjjmple.com
37	Tfuuutyhjghjhgjghjghjhgh	ubllljst@exajjjjmple.com
38	Tppppphjghjhgjghjghjhgh	ubllpppppp@exajjjjmple.com
39	mmmmmhjhgjghjghjhgh	ummmmmmp@exajjjjmple.com
40	iiiiiii	iiiiii@example.com
41	iiiyiiii	iiiiyii@example.com
42	iiiyiiii	iiiiyii@example.com
43	iiiyiiii	iiiiyii@example.com
44	iiiyiiii	iiiiyii@example.com
45	iiiggyiiii	iiiiggyii@example.com
46	iiiggyiiii	iiiiggyii@example.com
47	iiihhggyiiii	iiihhiggyii@example.com
48	iiiggyiiii	iiiiggyii@example.com
49	iighghiughgyiiii	iiighghghghghghyii@example.com
50	iiрррbbbb	bрррb@example.com
51	iizzz	bzzzzb@example.com
52	iizxzxzxxzyiiii	iizxxzxziiggyii@example.com
53	iizxppppxzyiiii	iizpppzxziiggyii@example.com
54	iiqqqxzyiiii	iizppqqziiggyii@example.com
\.


--
-- TOC entry 4819 (class 0 OID 0)
-- Dependencies: 218
-- Name: users_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.users_id_seq', 54, true);


--
-- TOC entry 4661 (class 2606 OID 24600)
-- Name: message_queue_log message_queue_log_pkey; Type: CONSTRAINT; Schema: data_exchange; Owner: postgres
--

ALTER TABLE ONLY data_exchange.message_queue_log
    ADD CONSTRAINT message_queue_log_pkey PRIMARY KEY (message_id);


--
-- TOC entry 4658 (class 2606 OID 16442)
-- Name: users users_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);


--
-- TOC entry 4659 (class 1259 OID 24614)
-- Name: idx_message_queue_log_transferred_time; Type: INDEX; Schema: data_exchange; Owner: postgres
--

CREATE INDEX idx_message_queue_log_transferred_time ON data_exchange.message_queue_log USING btree (transferred, message_time);


--
-- TOC entry 4663 (class 2620 OID 24613)
-- Name: users users_changes; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER users_changes AFTER INSERT OR DELETE OR UPDATE ON public.users FOR EACH ROW EXECUTE FUNCTION data_exchange.log_changes();


--
-- TOC entry 4662 (class 2606 OID 24607)
-- Name: message_queue_log_data fk_message; Type: FK CONSTRAINT; Schema: data_exchange; Owner: postgres
--

ALTER TABLE ONLY data_exchange.message_queue_log_data
    ADD CONSTRAINT fk_message FOREIGN KEY (message_id) REFERENCES data_exchange.message_queue_log(message_id) ON DELETE CASCADE;


-- Completed on 2026-02-02 18:07:53

--
-- PostgreSQL database dump complete
--

