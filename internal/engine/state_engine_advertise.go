package engine

import (
	"errors"
	"net"
	"strconv"
	"strings"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
)

// advertisement selects public SIP routing fields for one retained transport.
// It never derives routing from the peer socket or a private control address.
func (e *StateEngine) advertisement(transport string) (contact, sentBy string, err error) {
	return advertisement(e.cfg, transport)
}

func advertisement(cfg Config, transport string) (contact, sentBy string, err error) {
	if transport != "tls" && transport != "udp" && transport != "tcp" {
		return "", "", errors.New("unsupported advertisement transport")
	}
	var uri sip.Uri
	if err = sip.ParseUri(cfg.ContactURI, &uri); err != nil || uri.Host == "" || uri.Scheme != "sip" && uri.Scheme != "sips" {
		return "", "", errors.New("invalid public SIP Contact URI")
	}
	sentBy = cfg.AdvertiseAddress
	if transport != "tls" {
		sentBy = cfg.PlainAdvertiseAddress
		if sentBy == "" {
			host, _, splitErr := net.SplitHostPort(cfg.AdvertiseAddress)
			if splitErr != nil {
				return "", "", splitErr
			}
			sentBy = net.JoinHostPort(host, "5060")
		}
		uri.Scheme = "sip"
	}
	host, port, splitErr := net.SplitHostPort(sentBy)
	if splitErr != nil || host == "" || strings.ContainsAny(host, " \t\r\n") {
		return "", "", errors.New("invalid public SIP advertisement")
	}
	parsedPort, parseErr := strconv.Atoi(port)
	if parseErr != nil || parsedPort < 1 || parsedPort > 65535 {
		return "", "", errors.New("invalid public SIP advertisement port")
	}
	uri.Host, uri.Port = host, parsedPort
	params := uri.UriParams[:0]
	for _, param := range uri.UriParams {
		if !strings.EqualFold(param.K, "transport") {
			params = append(params, param)
		}
	}
	uri.UriParams = params
	uri.UriParams.Add("transport", transport)
	return uri.String(), net.JoinHostPort(host, strconv.Itoa(parsedPort)), nil
}

// initialInviteContact preserves secure dialog routing from the actual request.
func initialInviteContact(origin portable.Message, status int, contact string) (string, error) {
	facts, err := origin.Facts()
	if err != nil {
		return "", err
	}
	if !facts.IsRequest || facts.Method != sip.INVITE || facts.ToTag != "" || status <= 100 || status >= 300 {
		return contact, nil
	}
	var target sip.Uri
	if err := sip.ParseUri(facts.URI, &target); err != nil {
		return "", err
	}
	secure := strings.EqualFold(target.Scheme, "sips")
	routes, err := origin.HeaderValues("Record-Route")
	if err != nil {
		return "", err
	}
	if len(routes) > 0 {
		var route sip.Uri
		var params sip.HeaderParams
		if _, err := sip.ParseAddressValue(routes[0], &route, &params); err != nil {
			return "", err
		}
		secure = secure || strings.EqualFold(route.Scheme, "sips")
	} else if facts.HasContact {
		var remote sip.Uri
		if err := sip.ParseUri(facts.ContactURI, &remote); err != nil {
			return "", err
		}
		secure = secure || strings.EqualFold(remote.Scheme, "sips")
	}
	if !secure {
		return contact, nil
	}
	if origin.Transport() != "tls" {
		return "", errors.New("secure initial INVITE requires TLS")
	}
	var advertised sip.Uri
	if err := sip.ParseUri(contact, &advertised); err != nil {
		return "", err
	}
	advertised.Scheme = "sips"
	return advertised.String(), nil
}

func (r *stateReduction) buildResponse(c *stateContext, origin portable.Message, status int, phrase string, body []byte, headers []portable.Header) (portable.Message, error) {
	contact, _, err := r.engine.advertisement(origin.Transport())
	if err != nil {
		return portable.Message{}, err
	}
	if status >= 200 && status < 300 {
		facts, parseErr := origin.Facts()
		if parseErr == nil && facts.IsRequest && facts.Method == sip.REFER && facts.ToTag != "" {
			dialog, exists := c.Dialogs[facts.ToTag+"/"+facts.FromTag]
			if !exists || !portable.MatchDialogRequest(dialog, origin) || dialog.Contact == "" {
				return portable.Message{}, errors.New("successful in-dialog REFER response requires its retained native Contact")
			}
			contact = dialog.Contact
		}
	}
	contact, err = initialInviteContact(origin, status, contact)
	if err != nil {
		return portable.Message{}, err
	}
	return portable.BuildResponseWithHeaders(origin, status, phrase, body, c.Tag, contact, headers)
}
