package impl

import (
	"bytes"
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/samoslab/nebula/provider/node"
	util_hash "github.com/samoslab/nebula/util/hash"
	util_rsa "github.com/samoslab/nebula/util/rsa"
	"google.golang.org/grpc"

	pb "github.com/samoslab/nebula/tracker/register/client/pb"
)

func (self *ClientRegisterService) encrypt(data []byte) ([]byte, error) {
	return util_rsa.EncryptLong(self.PubKey, data, node.RSA_KEY_BYTES)
}

func TestDecrypt(t *testing.T) {
	var data, en, plain []byte
	var err error
	crs := NewClientRegisterService()
	data = []byte("test datatest datatest datatest datatest datatest datatest datatest datatest datatest datatest datatest datatest datatest datatest datatest datatest datatest datatest datatest datatest datatest datatest datatest datatest datatest datatest datatest datatest datatest datatest datatest datatest datatest datatest datatest datatest datatest datatest datatest data")
	en, err = crs.encrypt(data)
	if err != nil {
		t.Errorf("Failed.")
	}
	plain, err = crs.decrypt(en)
	if err != nil {
		t.Errorf("Failed.")
	}
	if !bytes.Equal(data, plain) {
		t.Errorf("Failed.")
	}
}

func TestRegister(t *testing.T) {
	pubKeyBytes, err := hex.DecodeString("3082010a0282010100bbc709dfc04cc17ec691ce64f118c9c3228058ed763484717d6d262c31fe2e6e928c7ebcc666fec8ab7bc6aa32c4f5023c959c671bda5a12d0671f00079cc070e427947b2de9c13cab1985ee3eacf96a77e6ddf9dfc5c5e2d88b0f34ac5759d2807add57735f987bd24b4d4c61724ecbc9be41dff409240ef7695435dce01dcbfdbdc8c1d023835ff946f938286e4ad18a03d23d86bad35c12601da70a60a6abee47f0f5040f2a47825a1b9c9a0b93d2a065bb2f3dba9fefc1aba3924389d47ccaaae51b454e7477d96eb70c7bd6525ace69a6c884ad5708a08759cfd02e01379ca63aae3340a576b212bd2ca20eb22c88c0b05527f7f5ee4929711062a336f70203010001")
	if err != nil {
		panic(err)
	}
	// pubKey, err := x509.ParsePKCS1PublicKey(pubKeyBytes)
	// if err != nil {
	// 	panic(err)
	// }
	priKeyBytes, err := hex.DecodeString("308204a40201000282010100bbc709dfc04cc17ec691ce64f118c9c3228058ed763484717d6d262c31fe2e6e928c7ebcc666fec8ab7bc6aa32c4f5023c959c671bda5a12d0671f00079cc070e427947b2de9c13cab1985ee3eacf96a77e6ddf9dfc5c5e2d88b0f34ac5759d2807add57735f987bd24b4d4c61724ecbc9be41dff409240ef7695435dce01dcbfdbdc8c1d023835ff946f938286e4ad18a03d23d86bad35c12601da70a60a6abee47f0f5040f2a47825a1b9c9a0b93d2a065bb2f3dba9fefc1aba3924389d47ccaaae51b454e7477d96eb70c7bd6525ace69a6c884ad5708a08759cfd02e01379ca63aae3340a576b212bd2ca20eb22c88c0b05527f7f5ee4929711062a336f7020301000102820100179fa9d1598b0e88d9887c73dc9526c502f12cbeeb311e3c7cf01f6e4df7d1759dc0492d8cb466776e838af1dca344c3bc458240c2934ce3b30e562b15ca15b7de2c5094d2a8e6e3b00eecf7953103cbee3bc04f70649c49b6be7ff23b805dcf8ad46a4657407e998d5265ee27104f7379f512aa8f341b323ad428810241bbddb7eaadd1c3fb339ac1e8e73de92c9ee71b6ba8a964eac72310bc9962212f496b7d1af69244daef3a6ad9fd2da9c1353fb269fadb1f9a707d8b7b2d28626d6d14c484a49ee6c5c0f9b117817d5213fed83ef7a52136ff6b050ff16b523ba6a290dabd333bdb28ff4097d96b017e6533d9ebe2a08f302243276e1d86d00ebaed4902818100db11ae2549247a6845a1b472133ef0b8eb724500a8338e80eccc0d24070472fd12d8b291103e9801b68b4a44d52f4217dd603b95bac5120489f8e17f84b55e7821e38eb67a9b2ae7237acd3fb7e8fbd7ed764964d4050b98d493f7c4430cbae4acbcd6e8a8ab1c8e463dde8cebc1d619b6cd7eae634663566b67bd84a5b3ef0502818100db6eea115f116dd0ffa220c3369bbdf56650982a5893bfc0da23b1aa45cad7992b2797a642947d1e050cc2831d9a5eeda96a7e2b0e3a632a27af8257cfc1f0a00aa1fb278cfc42b87b8b12ca438777d9ba9bbdbd3b558c5872eafdd450d630f26d513ff86337e8ad154c1a4d932189da0226f8572afc8c1d0f007c22d65556cb028180563e92a9b92ef445e2cbf3a7496e6904d424ab87c3b07074cf44d21391f3ad75769afb49e2f45191b315094b2a06ade58950de84670038c4b2b0d9ce0328082611696e00e729f96cffe9d3ae3730311de42dc25d409f2fde9e2a16cc1c7d81828f82d4b4c9da7ba6837ece03fab8d81a4d7e7f56165d5ab4661a7461297f3a2502818100a970d6119fe56775115072180b9ceb6c091b86c47c2d6ace522369d75f99282e40228c7977c40d7116d92981f163f8957052a9263a105fee774291559939dac2da33062b1e34d4987bdd821ee9523bfbc69ae842ad047c20f86bf8a0efe2d55cfd88d5eac942accaaa3d5fba33389ca7d92d9a6a44e94a904dbb441fea7d6f4d02818100bff019e78401f93b7174d9164384fd35df3f635a19488f68f915673f9fafda1777802cd025ab9bf16fac4da31165a539566bbacc36cb273491c388e5d4ad61e22d544f2a9ac3b8c2b3fb6b0ffd0eaf28ce115c959efb4540449a35351398ca9a8dd81cdc66a9e46f80520a8e11d8505ea1293679dbf0faaa114948e49dc9b785")
	if err != nil {
		panic(err)
	}
	// priKey, err := x509.ParsePKCS1PrivateKey(priKeyBytes)
	// if err != nil {
	// 	panic(err)
	// }
	nodeId := util_hash.Sha1(pubKeyBytes)
	conn, err := grpc.Dial("127.0.0.1:6677", grpc.WithInsecure())
	if err != nil {
		fmt.Printf("RPC Dial failed: %s\n", err.Error())
		return
	}
	defer conn.Close()
	crsc := pb.NewClientRegisterServiceClient(conn)

	trackerPubKeyBytes, err := getPublicKey(crsc)
	if err != nil {
		panic(err)
	}
	trackerPubKey, err := x509.ParsePKCS1PublicKey(trackerPubKeyBytes)
	if err != nil {
		panic(err)
	}
	pubKeyEnc, err := util_rsa.EncryptLong(trackerPubKey, pubKeyBytes, node.RSA_KEY_BYTES)
	if err != nil {
		panic(err)
	}
	emailEnc, err := util_rsa.EncryptLong(trackerPubKey, []byte("lijiangt@gmail.com"), node.RSA_KEY_BYTES)
	code, errMsg, err := register(crsc, nodeId, pubKeyEnc, emailEnc)
	if err != nil {
		panic(err)
	}
	if code != 0 {
		t.Error(errMsg)
	}
	t.Error(hex.EncodeToString(pubKeyBytes))
	t.Error(hex.EncodeToString(priKeyBytes))
}

func TestVerify(t *testing.T) {
	pubKeyBytes, err := hex.DecodeString("3082010a0282010100bbc709dfc04cc17ec691ce64f118c9c3228058ed763484717d6d262c31fe2e6e928c7ebcc666fec8ab7bc6aa32c4f5023c959c671bda5a12d0671f00079cc070e427947b2de9c13cab1985ee3eacf96a77e6ddf9dfc5c5e2d88b0f34ac5759d2807add57735f987bd24b4d4c61724ecbc9be41dff409240ef7695435dce01dcbfdbdc8c1d023835ff946f938286e4ad18a03d23d86bad35c12601da70a60a6abee47f0f5040f2a47825a1b9c9a0b93d2a065bb2f3dba9fefc1aba3924389d47ccaaae51b454e7477d96eb70c7bd6525ace69a6c884ad5708a08759cfd02e01379ca63aae3340a576b212bd2ca20eb22c88c0b05527f7f5ee4929711062a336f70203010001")
	if err != nil {
		panic(err)
	}
	// pubKey, err := x509.ParsePKCS1PublicKey(pubKeyBytes)
	// if err != nil {
	// 	panic(err)
	// }
	priKeyBytes, err := hex.DecodeString("308204a40201000282010100bbc709dfc04cc17ec691ce64f118c9c3228058ed763484717d6d262c31fe2e6e928c7ebcc666fec8ab7bc6aa32c4f5023c959c671bda5a12d0671f00079cc070e427947b2de9c13cab1985ee3eacf96a77e6ddf9dfc5c5e2d88b0f34ac5759d2807add57735f987bd24b4d4c61724ecbc9be41dff409240ef7695435dce01dcbfdbdc8c1d023835ff946f938286e4ad18a03d23d86bad35c12601da70a60a6abee47f0f5040f2a47825a1b9c9a0b93d2a065bb2f3dba9fefc1aba3924389d47ccaaae51b454e7477d96eb70c7bd6525ace69a6c884ad5708a08759cfd02e01379ca63aae3340a576b212bd2ca20eb22c88c0b05527f7f5ee4929711062a336f7020301000102820100179fa9d1598b0e88d9887c73dc9526c502f12cbeeb311e3c7cf01f6e4df7d1759dc0492d8cb466776e838af1dca344c3bc458240c2934ce3b30e562b15ca15b7de2c5094d2a8e6e3b00eecf7953103cbee3bc04f70649c49b6be7ff23b805dcf8ad46a4657407e998d5265ee27104f7379f512aa8f341b323ad428810241bbddb7eaadd1c3fb339ac1e8e73de92c9ee71b6ba8a964eac72310bc9962212f496b7d1af69244daef3a6ad9fd2da9c1353fb269fadb1f9a707d8b7b2d28626d6d14c484a49ee6c5c0f9b117817d5213fed83ef7a52136ff6b050ff16b523ba6a290dabd333bdb28ff4097d96b017e6533d9ebe2a08f302243276e1d86d00ebaed4902818100db11ae2549247a6845a1b472133ef0b8eb724500a8338e80eccc0d24070472fd12d8b291103e9801b68b4a44d52f4217dd603b95bac5120489f8e17f84b55e7821e38eb67a9b2ae7237acd3fb7e8fbd7ed764964d4050b98d493f7c4430cbae4acbcd6e8a8ab1c8e463dde8cebc1d619b6cd7eae634663566b67bd84a5b3ef0502818100db6eea115f116dd0ffa220c3369bbdf56650982a5893bfc0da23b1aa45cad7992b2797a642947d1e050cc2831d9a5eeda96a7e2b0e3a632a27af8257cfc1f0a00aa1fb278cfc42b87b8b12ca438777d9ba9bbdbd3b558c5872eafdd450d630f26d513ff86337e8ad154c1a4d932189da0226f8572afc8c1d0f007c22d65556cb028180563e92a9b92ef445e2cbf3a7496e6904d424ab87c3b07074cf44d21391f3ad75769afb49e2f45191b315094b2a06ade58950de84670038c4b2b0d9ce0328082611696e00e729f96cffe9d3ae3730311de42dc25d409f2fde9e2a16cc1c7d81828f82d4b4c9da7ba6837ece03fab8d81a4d7e7f56165d5ab4661a7461297f3a2502818100a970d6119fe56775115072180b9ceb6c091b86c47c2d6ace522369d75f99282e40228c7977c40d7116d92981f163f8957052a9263a105fee774291559939dac2da33062b1e34d4987bdd821ee9523bfbc69ae842ad047c20f86bf8a0efe2d55cfd88d5eac942accaaa3d5fba33389ca7d92d9a6a44e94a904dbb441fea7d6f4d02818100bff019e78401f93b7174d9164384fd35df3f635a19488f68f915673f9fafda1777802cd025ab9bf16fac4da31165a539566bbacc36cb273491c388e5d4ad61e22d544f2a9ac3b8c2b3fb6b0ffd0eaf28ce115c959efb4540449a35351398ca9a8dd81cdc66a9e46f80520a8e11d8505ea1293679dbf0faaa114948e49dc9b785")
	if err != nil {
		panic(err)
	}
	priKey, err := x509.ParsePKCS1PrivateKey(priKeyBytes)
	if err != nil {
		panic(err)
	}
	nodeId := util_hash.Sha1(pubKeyBytes)
	conn, err := grpc.Dial("127.0.0.1:6677", grpc.WithInsecure())
	if err != nil {
		fmt.Printf("RPC Dial failed: %s\n", err.Error())
		return
	}
	defer conn.Close()
	crsc := pb.NewClientRegisterServiceClient(conn)
	code, errMsg, err := verifyContactEmail(crsc, nodeId, "7winjgw8	", priKey)
	if err != nil {
		t.Error(err)
	}
	if code != 0 {
		t.Error(errMsg)
	}
}

func getPublicKey(client pb.ClientRegisterServiceClient) (pubKey []byte, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := client.GetPublicKey(ctx, &pb.GetPublicKeyReq{})
	if err != nil {
		return nil, err
	}
	return resp.PublicKey, nil
}

func register(client pb.ClientRegisterServiceClient, nodeId []byte, publicKeyEnc []byte,
	contactEmailEnc []byte) (code uint32, errMsg string, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req := &pb.RegisterReq{ //Timestamp: uint64(time.Now().Unix()),
		NodeId:          nodeId,
		PublicKeyEnc:    publicKeyEnc,
		ContactEmailEnc: contactEmailEnc}
	// req.SignReq(priKey)
	resp, err := client.Register(ctx, req)
	if err != nil {
		return 1000, "", err
	}
	return resp.Code, resp.ErrMsg, nil
}

func verifyContactEmail(client pb.ClientRegisterServiceClient, nodeId []byte, verifyCode string, priKey *rsa.PrivateKey) (code uint32, errMsg string, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req := &pb.VerifyContactEmailReq{NodeId: nodeId,
		Timestamp:  uint64(time.Now().Unix()),
		VerifyCode: verifyCode}
	req.SignReq(priKey)
	resp, err := client.VerifyContactEmail(ctx, req)
	if err != nil {
		return 0, "", err
	}
	return resp.Code, resp.ErrMsg, nil
}

func resendVerifyCode(client pb.ClientRegisterServiceClient) (success bool, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	node := node.LoadFormConfig()
	req := &pb.ResendVerifyCodeReq{NodeId: node.NodeId,
		Timestamp: uint64(time.Now().Unix())}
	req.SignReq(node.PriKey)
	resp, err := client.ResendVerifyCode(ctx, req)
	if err != nil {
		return false, err
	}
	return resp.Success, nil
}
