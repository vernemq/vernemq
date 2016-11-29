/*
Copyright (c) 2012,2013 Roger Light <roger@atchoo.org>
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
   this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright
   notice, this list of conditions and the following disclaimer in the
   documentation and/or other materials provided with the distribution.
3. Neither the name of mosquitto nor the names of its
   contributors may be used to endorse or promote products derived from
   this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.
*/


#include <errno.h>
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <openssl/buffer.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef WIN32
#  include <process.h>
#	ifndef __cplusplus
#		define bool char
#		define true 1
#		define false 0
#	endif
#   define snprintf sprintf_s
#	include <io.h>
#else
#  include <stdbool.h>
#  include <unistd.h>
#  include <termios.h>
#endif

#if OPENSSL_VERSION_NUMBER < 0x10100000L
/* Compatibility functionality for pre 1.1 openssl versions. */

static void *OPENSSL_zalloc(size_t num)
{
	void *ret = OPENSSL_malloc(num);

	if (ret != NULL)
		memset(ret, 0, num);
	return ret;
}

EVP_MD_CTX *EVP_MD_CTX_new(void)
{
	return OPENSSL_zalloc(sizeof(EVP_MD_CTX));
}

void EVP_MD_CTX_free(EVP_MD_CTX *ctx)
{
	EVP_MD_CTX_cleanup(ctx);
	OPENSSL_free(ctx);
}
#endif

#define MAX_BUFFER_LEN 1024
#define SALT_LEN 12


int base64_encode(unsigned char *in, unsigned int in_len, char **encoded)
{
	BIO *bmem, *b64;
	BUF_MEM *bptr;

	b64 = BIO_new(BIO_f_base64());
	BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);
	bmem = BIO_new(BIO_s_mem());
	b64 = BIO_push(b64, bmem);
	BIO_write(b64, in, in_len);
	if(BIO_flush(b64) != 1){
		BIO_free_all(b64);
		return 1;
	}
	BIO_get_mem_ptr(b64, &bptr);
	*encoded = calloc(bptr->length+1, 1);
	if(!(*encoded)){
		BIO_free_all(b64);
		return 1;
	}
	memcpy(*encoded, bptr->data, bptr->length);
	(*encoded)[bptr->length] = '\0';
	BIO_free_all(b64);

	return 0;
}


void print_usage(void)
{
	printf("vmq-passwd is a tool for managing password files for VerneMQ.\n\n");
	printf("Usage: vmq-passwd [-c | -D] passwordfile username\n");
	printf("       vmq-passwd -U passwordfile\n");
	printf(" -c : create a new password file. This will overwrite existing files.\n");
	printf(" -D : delete the username rather than adding/updating its password.\n");
	printf(" -U : update a plain text password file to use hashed passwords.\n");
}

int output_new_password(FILE *fptr, const char *username, const char *password)
{
	int rc;
	unsigned char salt[SALT_LEN];
	char *salt64 = NULL, *hash64 = NULL;
	unsigned char hash[EVP_MAX_MD_SIZE];
	unsigned int hash_len;
	const EVP_MD *digest;
	EVP_MD_CTX *context;
	context = EVP_MD_CTX_new();

	rc = RAND_bytes(salt, SALT_LEN);
	if(!rc){
		fprintf(stderr, "Error: Insufficient entropy available to perform password generation.\n");
		return 1;
	}

	rc = base64_encode(salt, SALT_LEN, &salt64);
	if(rc){
		if(salt64) free(salt64);
		fprintf(stderr, "Error: Unable to encode salt.\n");
		return 1;
	}


	digest = EVP_get_digestbyname("sha512");
	if(!digest){
		fprintf(stderr, "Error: Unable to create openssl digest.\n");
		return 1;
	}

	EVP_MD_CTX_init(context);
	EVP_DigestInit_ex(context, digest, NULL);
	EVP_DigestUpdate(context, password, strlen(password));
	EVP_DigestUpdate(context, salt, SALT_LEN);
	EVP_DigestFinal_ex(context, hash, &hash_len);
	EVP_MD_CTX_free(context);

	rc = base64_encode(hash, hash_len, &hash64);
	if(rc){
		if(salt64) free(salt64);
		if(hash64) free(hash64);
		fprintf(stderr, "Error: Unable to encode hash.\n");
		return 1;
	}

	fprintf(fptr, "%s:$6$%s$%s\n", username, salt64, hash64);
	free(salt64);
	free(hash64);

	return 0;
}

int delete_pwuser(FILE *fptr, FILE *ftmp, const char *username)
{
	char buf[MAX_BUFFER_LEN];
	char lbuf[MAX_BUFFER_LEN], *token;
	bool found = false;

	while(!feof(fptr) && fgets(buf, MAX_BUFFER_LEN, fptr)){
		memcpy(lbuf, buf, MAX_BUFFER_LEN);
		token = strtok(lbuf, ":");
		if(strcmp(username, token)){
			fprintf(ftmp, "%s", buf);
		}else{
			found = true;
		}
	}
	if(!found){
		fprintf(stderr, "Warning: User %s not found in password file.\n", username);
	}
	return 0;
}

int update_file(FILE *fptr, FILE *ftmp)
{
	char buf[MAX_BUFFER_LEN];
	char lbuf[MAX_BUFFER_LEN];
	char *username, *password;
	int rc;
	int len;

	while(!feof(fptr) && fgets(buf, MAX_BUFFER_LEN, fptr)){
		memcpy(lbuf, buf, MAX_BUFFER_LEN);
		username = strtok(lbuf, ":");
		password = strtok(NULL, ":");
		if(password){
			len = strlen(password);
			while(len && (password[len-1] == '\n' || password[len-1] == '\r')){
				password[len-1] = '\0';
				len = strlen(password);
			}
			rc = output_new_password(ftmp, username, password);
			if(rc) return rc;
		}else{
			fprintf(ftmp, "%s", username);
		}
	}
	return 0;
}

int update_pwuser(FILE *fptr, FILE *ftmp, const char *username, const char *password)
{
	char buf[MAX_BUFFER_LEN];
	char lbuf[MAX_BUFFER_LEN], *token;
	bool found = false;
	int rc = 1;

	while(!feof(fptr) && fgets(buf, MAX_BUFFER_LEN, fptr)){
		memcpy(lbuf, buf, MAX_BUFFER_LEN);
		token = strtok(lbuf, ":");
		if(strcmp(username, token)){
			fprintf(ftmp, "%s", buf);
		}else{
			rc = output_new_password(ftmp, username, password);
			found = true;
		}
	}
	if(found){
		return rc;
	}else{
		return output_new_password(ftmp, username, password);
	}
}

int gets_quiet(char *s, int len)
{
#ifdef WIN32
	HANDLE h;
	DWORD con_orig, con_quiet;
	DWORD read_len = 0;

	memset(s, 0, len);
	h  = GetStdHandle(STD_INPUT_HANDLE);
	GetConsoleMode(h, &con_orig);
	con_quiet &= ~ENABLE_ECHO_INPUT;
	con_quiet |= ENABLE_LINE_INPUT;
	SetConsoleMode(h, con_quiet);
	if(!ReadConsole(h, s, len, &read_len, NULL)){
		SetConsoleMode(h, con_orig);
		return 1;
	}
	while(s[strlen(s)-1] == 10 || s[strlen(s)-1] == 13){
		s[strlen(s)-1] = 0;
	}
	if(strlen(s) == 0){
		return 1;
	}
	SetConsoleMode(h, con_orig);

	return 0;
#else
	struct termios ts_quiet, ts_orig;
	char *rs;

	memset(s, 0, len);
	tcgetattr(0, &ts_orig);
	ts_quiet = ts_orig;
	ts_quiet.c_lflag &= ~(ECHO | ICANON);
	tcsetattr(0, TCSANOW, &ts_quiet);

	rs = fgets(s, len, stdin);
	tcsetattr(0, TCSANOW, &ts_orig);

	if(!rs){
		return 1;
	}else{
		while(s[strlen(s)-1] == 10 || s[strlen(s)-1] == 13){
			s[strlen(s)-1] = 0;
		}
		if(strlen(s) == 0){
			return 1;
		}
	}
	return 0;
#endif
}

int get_password(char *password, int len)
{
	char pw1[MAX_BUFFER_LEN], pw2[MAX_BUFFER_LEN];

	printf("Password: ");
	if(gets_quiet(pw1, MAX_BUFFER_LEN)){
		fprintf(stderr, "Error: Empty password.\n");
		return 1;
	}
	printf("\n");

	printf("Reenter password: ");
	if(gets_quiet(pw2, MAX_BUFFER_LEN)){
		fprintf(stderr, "Error: Empty password.\n");
		return 1;
	}
	printf("\n");

	if(strcmp(pw1, pw2)){
		fprintf(stderr, "Error: Passwords do not match.\n");
		return 1;
	}

	strncpy(password, pw1, len);
	return 0;
}

int copy_contents(FILE *src, FILE *dest)
{
	char buf[MAX_BUFFER_LEN];
	int len;

	rewind(src);
	rewind(dest);
	
#ifdef WIN32
	_chsize(fileno(dest), 0);
#else
	if(ftruncate(fileno(dest), 0)) return 1;
#endif

	while(!feof(src)){
		len = fread(buf, 1, MAX_BUFFER_LEN, src);
		if(len > 0){
			if(fwrite(buf, 1, len, dest) != len){
				return 1;
			}
		}else{
			return !feof(src);
		}
	}
	return 0;
}

int create_backup(const char *backup_file, FILE *fptr)
{
	FILE *fbackup;

	fbackup = fopen(backup_file, "wt");
	if(!fbackup){
		fprintf(stderr, "Error creating backup password file \"%s\", not continuing.\n", backup_file);
		return 1;
	}
	if(copy_contents(fptr, fbackup)){
		fprintf(stderr, "Error copying data to backup password file \"%s\", not continuing.\n", backup_file);
		fclose(fbackup);
		return 1;
	}
	fclose(fbackup);
	rewind(fptr);
	return 0;
}
void handle_sigint(int signal)
{
#ifndef WIN32
	struct termios ts;

	tcgetattr(0, &ts);
	ts.c_lflag |= ECHO | ICANON;
	tcsetattr(0, TCSANOW, &ts);
#endif
	exit(0);
}

int main(int argc, char *argv[])
{
	char *password_file = NULL;
	char *username = NULL;
	bool create_new = false;
	bool delete_user = false;
	FILE *fptr, *ftmp;
	char password[MAX_BUFFER_LEN];
	int rc;
	bool do_update_file = false;
	char *backup_file;

	signal(SIGINT, handle_sigint);
	signal(SIGTERM, handle_sigint);

	OpenSSL_add_all_digests();

	if(argc == 4){
		if(!strcmp(argv[1], "-c")){
			create_new = true;
		}else if(!strcmp(argv[1], "-D")){
			delete_user = true;
		}
		password_file = argv[2];
		username = argv[3];
	}else if(argc == 3){
		if(!strcmp(argv[1], "-U")){
			do_update_file = true;
			password_file = argv[2];
		}else{
			password_file = argv[1];
			username = argv[2];
		}
	}else{
		print_usage();
		return 1;
	}

	if(create_new){
		rc = get_password(password, 1024);
		if(rc) return rc;
		fptr = fopen(password_file, "wt");
		if(!fptr){
			fprintf(stderr, "Error: Unable to open file %s for writing. %s.\n", password_file, strerror(errno));
			return 1;
		}
		rc = output_new_password(fptr, username, password);
		fclose(fptr);
		return rc;
	}else{
		fptr = fopen(password_file, "r+t");
		if(!fptr){
			fprintf(stderr, "Error: Unable to open password file %s. %s.\n", password_file, strerror(errno));
			return 1;
		}

		backup_file = malloc(strlen(password_file)+5);
		snprintf(backup_file, strlen(password_file)+5, "%s.tmp", password_file);

		if(create_backup(backup_file, fptr)){
			fclose(fptr);
			free(backup_file);
			return 1;
		}

		ftmp = tmpfile();
		if(!ftmp){
			fprintf(stderr, "Error: Unable to open temporary file. %s.\n", strerror(errno));
			fclose(fptr);
			free(backup_file);
			return 1;
		}
		if(delete_user){
			rc = delete_pwuser(fptr, ftmp, username);
		}else if(do_update_file){
			rc = update_file(fptr, ftmp);
		}else{
			rc = get_password(password, 1024);
			if(rc){
				fclose(fptr);
				fclose(ftmp);
				unlink(backup_file);
				free(backup_file);
				return rc;
			}
			/* Update password for individual user */
			rc = update_pwuser(fptr, ftmp, username, password);
		}
		if(rc){
			fclose(fptr);
			fclose(ftmp);
			unlink(backup_file);
			free(backup_file);
			return rc;
		}

		if(copy_contents(ftmp, fptr)){
			fclose(fptr);
			fclose(ftmp);
			fprintf(stderr, "Error occurred updating password file.\n");
			fprintf(stderr, "Password file may be corrupt, check the backup file: %s.\n", backup_file);
			free(backup_file);
			return 1;
		}
		fclose(fptr);
		fclose(ftmp);

		/* Everything was ok so backup no longer needed. May contain old
		 * passwords so shouldn't be kept around. */
		unlink(backup_file);
		free(backup_file);
	}

	return 0;
}
